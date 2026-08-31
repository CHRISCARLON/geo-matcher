import functools
import logging
import pathlib
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeAlias, TypeVar, cast, runtime_checkable

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sedonadb.context import SedonaContext

from .config import BBox, DatasetConfig, GeometryType, LhsKind
from .explain import log_plan
from .join_sql import (
    OVERLAP_EXPR_CORRIDOR,
    bbox_nearest_filters,
    bbox_pruner,
    col_fragment,
    fill_spatial_filter,
    sql_ident,
    usrn_spatial_filter,
)
from .logger import get_logger

log: logging.Logger = get_logger()


# ---------------------------------------------------------------------------
# Join Mode
# ---------------------------------------------------------------------------


_MAX_BBOX_AREA_M2: int = 3_000_000_000  # 3,000 km² — approx Greater London


@dataclass(frozen=True)
class FilteredMode:
    """Bbox-scoped join — single SQL query against a city or custom bbox."""

    bbox: BBox

    def __post_init__(self) -> None:
        if len(self.bbox) != 4:
            raise ValueError(
                f"bbox must have 4 elements [xmin, ymin, xmax, ymax], got {self.bbox!r}"
            )
        xmin, ymin, xmax, ymax = self.bbox
        if xmin >= xmax or ymin >= ymax:
            raise ValueError(
                f"Invalid bbox {self.bbox!r}: require xmin<xmax and ymin<ymax "
                "(expected order is [xmin, ymin, xmax, ymax])."
            )
        area = (xmax - xmin) * (ymax - ymin)
        if area > _MAX_BBOX_AREA_M2:
            raise ValueError(
                f"Bbox area {area / 1e6:.0f} km² exceeds the {_MAX_BBOX_AREA_M2 / 1e6:.0f} km² "
                f"limit (~Greater London). Use NationalMode for large areas."
            )


@dataclass(frozen=True)
class NationalMode:
    """Full national join — RHS split into *n_chunks* chunks processed one at a time."""

    n_chunks: int = 80


JoinMode: TypeAlias = FilteredMode | NationalMode

_DEFAULT_MODE: JoinMode = NationalMode()


# ---------------------------------------------------------------------------
# Join Function Registry
# ---------------------------------------------------------------------------


@runtime_checkable
class JoinFn(Protocol):
    """Contract every join implementation must satisfy."""

    def __call__(
        self,
        sd: SedonaContext,
        usrn_parquet: pathlib.Path,
        rhs_config: DatasetConfig,
        *,
        mode: JoinMode = ...,
        explain: bool = ...,
        distance_m: float = ...,
        phase3_distance_m: float | None = ...,
        rhs_id_col: str | None = ...,
        output_path: pathlib.Path | None = ...,
        overlap_threshold: float = ...,
        usrn_line_parquet: pathlib.Path | None = ...,
        phase4_tolerance_m: float = ...,
    ) -> pa.Table: ...


# The registry dictionary that get_join reads from — keyed by (LhsKind, GeometryType)
# so the registered join strategies can never drift from either enum, and USRN and
# UPRN can each register a strategy for the same RHS geometry type (e.g. both have
# a "polygon" join) without colliding on a single GeometryType key.
_registry: dict[tuple[LhsKind, GeometryType], JoinFn] = {}
_J = TypeVar("_J", bound=JoinFn)


def register(lhs: LhsKind | str, geometry: GeometryType | str) -> Callable[[_J], _J]:
    """Register a join function under the *lhs* base dataset and *geometry* RHS type.

    Raises at import time if either argument isn't a valid enum member (or its
    plain-string value), so a typo in a @register decorator can never produce
    an unreachable join.
    """
    try:
        lhs_kind: LhsKind = LhsKind(lhs)
    except ValueError:
        raise ValueError(
            f"Cannot register join for lhs={lhs!r}: not a LhsKind. "
            f"Available: {[member.value for member in LhsKind]}"
        ) from None
    try:
        geometry_type: GeometryType = GeometryType(geometry)
    except ValueError:
        raise ValueError(
            f"Cannot register join {geometry!r}: not a GeometryType. "
            f"Available: {[g.value for g in GeometryType]}"
        ) from None

    # The inner function that registers the function against
    # what is in @register(LhsKind.USRN, GeometryType.POINT)
    def decorator(fn: _J) -> _J:
        _registry[(lhs_kind, geometry_type)] = fn
        return fn

    return decorator


def get_join(lhs: LhsKind | str, geometry: GeometryType | str) -> JoinFn:
    """Look up the join registered for an (lhs, geometry) pair.

    Accepts either enum members or their plain-string values — StrEnum members
    hash and compare equal to their values, so a plain-string tuple matches a
    tuple of enum members in the registry.
    """
    key = (lhs, geometry)
    if key not in _registry:
        raise KeyError(
            f"No join registered for lhs={lhs!r}, mode={geometry!r}. "
            f"Available: {[(str(lk), str(gk)) for (lk, gk) in _registry]}"
        )
    return _registry[(LhsKind(lhs), GeometryType(geometry))]


# ---------------------------------------------------------------------------
# Session configuration
# ---------------------------------------------------------------------------


def configure_sedona_session(sd: SedonaContext, target_partitions: int = 4) -> None:
    """Apply DataFusion/Sedona tuning once after sedonadb.connect().

    target_partitions: caps RoundRobinBatch fan-out — prevents CPU saturation on national joins.
    repartition_probe_side=false: probe side is already pre-filtered per chunk; reshuffling adds a
    memcpy round-trip for no gain on hilbert-sorted input.
    concurrent_build_side_collection=false: build side per chunk is ~50k rows / ~24 MB; parallel
    collection overhead exceeds the gain at that size.
    """
    sd.sql(
        f"SET datafusion.execution.target_partitions TO {target_partitions}"
    ).execute()
    sd.sql("SET sedona.spatial_join.repartition_probe_side TO false").execute()
    sd.sql(
        "SET sedona.spatial_join.concurrent_build_side_collection TO false"
    ).execute()


@functools.lru_cache(maxsize=1)
def _duck() -> duckdb.DuckDBPyConnection:
    """Shared DuckDB connection for the overlap/dedup post-processors.

    The spatial extension is installed and loaded exactly once per process here.
    The post-processors re-``register("t", table)`` on each call (which rebinds the
    view), so reusing this connection avoids paying ``LOAD spatial`` per batch —
    in a national line join that is dozens-to-hundreds of avoided loads.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


# ---------------------------------------------------------------------------
# Arrow, chunking and view helpers
# ---------------------------------------------------------------------------


def _bbox_col_indices(pf: pq.ParquetFile) -> dict[str, int]:
    """Map bbox sub-field name → column index in the parquet row-group metadata."""
    first_rg = pf.metadata.row_group(0)
    path_to_idx = {
        first_rg.column(i).path_in_schema: i for i in range(first_rg.num_columns)
    }
    log.debug("bbox column indices: %s", path_to_idx)
    return {f: path_to_idx[f"bbox.{f}"] for f in ("xmin", "ymin", "xmax", "ymax")}


def _split_into_chunks(n_row_groups: int, n_chunks: int) -> list[list[int]]:
    """Partition row-group indices into at most *n_chunks* contiguous chunks.

    Each row group lands in exactly one chunk, which is what makes the per-chunk
    match-rate counters summable into global figures.
    """
    rgs_per_chunk = max(1, (n_row_groups + n_chunks - 1) // n_chunks)
    return [
        list(range(s, min(s + rgs_per_chunk, n_row_groups)))
        for s in range(0, n_row_groups, rgs_per_chunk)
    ]


def _row_group_envelope(
    pf: pq.ParquetFile, chunk_rgs: list[int], bbox_idx: dict[str, int]
) -> BBox:
    """Derive the spatial envelope of a chunk's row groups from their column statistics."""
    rgs = [pf.metadata.row_group(i) for i in chunk_rgs]
    return (
        min(rg.column(bbox_idx["xmin"]).statistics.min for rg in rgs),
        min(rg.column(bbox_idx["ymin"]).statistics.min for rg in rgs),
        max(rg.column(bbox_idx["xmax"]).statistics.max for rg in rgs),
        max(rg.column(bbox_idx["ymax"]).statistics.max for rg in rgs),
    )


def _table_envelope(table: pa.Table) -> BBox:
    """Derive an Arrow table's spatial envelope from its ``bbox`` struct column."""
    bbox_col = table.column("bbox")
    return (
        pc.min(pc.struct_field(bbox_col, "xmin")).as_py(),
        pc.min(pc.struct_field(bbox_col, "ymin")).as_py(),
        pc.max(pc.struct_field(bbox_col, "xmax")).as_py(),
        pc.max(pc.struct_field(bbox_col, "ymax")).as_py(),
    )


def _normalise_arrow(table: pa.Table) -> pa.Table:
    """Cast Sedona's Utf8View and non-nullable bools to plain utf8 / nullable bool.

    Sedona and DuckDB emit different Arrow types for the same logical columns; both
    output paths must match before results can be concatenated or written to a shared
    ParquetWriter.
    """
    new_fields = [
        pa.field(f.name, pa.string(), nullable=True)
        if f.type == pa.string_view()
        else pa.field(f.name, f.type, nullable=True)
        if f.type == pa.bool_() and not f.nullable
        else f
        for f in table.schema
    ]
    target = pa.schema(new_fields)
    return table.cast(target)


def _anti_join_mask(
    table: pa.Table, id_col: str, exclude_ids: pa.Array | pa.ChunkedArray
) -> pa.Array | pa.ChunkedArray:
    """Boolean mask — True where ``table[id_col]`` is NOT in ``exclude_ids``.

    Vectorised replacement for per-row Python ``[v not in s for v in col.to_pylist()]``
    pattern: ``pc.is_in`` runs in Arrow's C++ instead of the Python interpreter.
    An empty ``exclude_ids`` keeps every row (all-True mask).
    """
    col = table.column(id_col)
    if len(exclude_ids) == 0:
        return pa.array([True] * len(table), type=pa.bool_())
    return pc.invert(pc.is_in(col, value_set=exclude_ids))


def _distinct_ids(parts: list[pa.Table], id_col: str, id_type: pa.DataType) -> pa.Array:
    """Distinct ``id_col`` values across a list of result tables, as an Arrow array.

    Stays inside Arrow/C++ rather than boxing every id to a Python object — the
    same reasoning as ``_anti_join_mask``'s docstring, just applied on the way in
    instead of the way out. *id_type* covers the all-empty case (``parts`` empty,
    or every part in it empty): there is then no column left to infer the type
    from, so the caller passes it explicitly — the id column's type on the RHS
    table these parts were derived from.
    """
    chunks = [chunk for t in parts for chunk in t.column(id_col).chunks]
    return pc.unique(pa.chunked_array(chunks, type=id_type))


def _union_ids(*arrays: pa.Array) -> pa.Array:
    """Distinct union of several already-deduplicated id arrays (``_distinct_ids`` output)."""
    return pc.unique(pa.concat_arrays(arrays))


def _intersect_len(a: pa.Array, b: pa.Array) -> int:
    """Count of ids common to both (already-deduplicated) id arrays."""
    return len(a.filter(pc.is_in(a, value_set=b)))


def _register_rhs_view(sd: SedonaContext, table: pa.Table, rhs_view: str) -> None:
    """Register an in-memory RHS table as *rhs_view* with a decoded geometry column.

    The Arrow table carries geometry as WKB, so it lands as ``rhs_raw`` first and is
    then re-projected through ``ST_GeomFromWKB`` into the view the join templates read.
    """
    non_geom = ", ".join(f'"{c}"' for c in table.schema.names if c != "geometry")
    sd.create_data_frame(table).to_view("rhs_raw", overwrite=True)
    sd.sql(
        f"SELECT {non_geom},"
        f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
    ).to_view(rhs_view, overwrite=True)


def _register_neighbours_view(
    sd: SedonaContext, features: pa.Table, matched: pa.Table, id_col: str
) -> int:
    """Register already-matched RHS features as the ``neighbours`` view for Phase 4.

    Each matched feature contributes its geometry plus the single best USRN it
    resolved to — lowest ``match_phase`` first (an intersection beats a corridor beats
    a nearest hit), then shortest ``distance_m``.

    Returns the number of neighbour rows registered (0 if there is nothing to seed from).
    """
    if not len(matched) or not len(features):
        return 0
    con = _duck()
    con.register("_feat", features)
    con.register("_matched", matched)

    # Find the best, single USRN match for a
    # given feature from phase 1,2,3
    neighbours = con.execute(f"""
        WITH best_match AS (
            SELECT "{id_col}" AS _id, usrn, street_type,
                ROW_NUMBER() OVER (
                    PARTITION BY "{id_col}" ORDER BY match_phase, distance_m, usrn
                ) AS _rn
            FROM _matched
        )
        SELECT f.geometry AS geometry, b.usrn AS usrn, b.street_type AS street_type
        FROM best_match AS b
        JOIN _feat AS f ON f."{id_col}" = b._id
        WHERE b._rn = 1
    """).to_arrow_table()
    if not len(neighbours):
        return 0
    sd.create_data_frame(neighbours).to_view("neighbours_raw", overwrite=True)

    # Register a view for phase 4
    # usrn, street type and the geometry of the matched feautre
    # TODO: Return the feature's column ID as well
    # So we can see what features bridges the usrns to the new target feature
    sd.sql(
        "SELECT usrn, street_type,"
        " ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM neighbours_raw"
    ).to_view("neighbours", overwrite=True)
    return len(neighbours)


# ---------------------------------------------------------------------------
# Phase post-processors
#
# One reducer per phase, each with its own ``match_phase``:
#   Phase 1  _phase1_score_overlap   — score every touching pair, keep them all
#   Phase 2  _phase2_select_corridors — score, drop Phase 1's pairs, rank, filter
#   Phase 3  _nearest_dedup          — closest USRN per feature
#   Phase 4  _nearest_dedup(phase=4) — closest matched neighbour's USRN
# ---------------------------------------------------------------------------


def _phase1_score_overlap(table: pa.Table, distance_m: float) -> pa.Table:
    """Add ``overlap_length_pct`` and ``match_phase=1`` to Phase 1 results — all pairs kept.

    Touching a centreline is definitive, so there is no threshold and no ranking here.
    The score is carried purely so consumers can see how much of the line the crossed
    street's corridor actually covers.
    """
    con = _duck()
    con.register("t", table)
    expr = OVERLAP_EXPR_CORRIDOR.format(denom=2 * distance_m)
    result = con.execute(f"""
        SELECT * EXCLUDE (_u_geom, _s_geom),
            {expr} AS overlap_length_pct
        FROM t
    """).to_arrow_table()
    return result.append_column(
        "match_phase", pa.array([1] * len(result), type=pa.int8())
    )


def _phase2_select_corridors(
    table: pa.Table,
    id_col: str,
    distance_m: float,
    min_overlap: float = 0.10,
    exclude_pairs: pa.Table | None = None,
) -> pa.Table:
    """Keep the best USRN corridor match(es) per Phase 2 RHS feature.

    Drops features whose best candidate is below ``min_overlap`` (default 10 %) —
    a sub-threshold match is a crossing, not a corridor relationship. Among passing
    features, keeps all USRNs within 80 % of the best overlap so a feature straddling
    two streets gets both.

    ``_u_geom`` is the pre-buffered corridor polygon (usrns_line.geometry), so the
    overlap is scored against the same corridor the Phase 2 join matched against.

    *exclude_pairs* is the Phase 1 result. Phase 2 runs over every feature, not just
    Phase 1's leftovers, so a feature that crossed a centreline gets that same USRN
    back through the buffer. Those ``(feature, usrn)`` pairs are anti-joined away
    **before** scoring, for two reasons: emitting them again would duplicate a pair
    Phase 1 already reported with stronger evidence, and leaving them in the ranking
    window lets the crossed street set the 80 % bar — which cuts the genuinely
    adjacent streets this phase exists to find. ``None`` or an empty table leaves
    behaviour identical to an unfiltered Phase 2.
    """
    con = _duck()
    con.register("t", table)
    expr = OVERLAP_EXPR_CORRIDOR.format(denom=2 * distance_m)
    if exclude_pairs is not None and len(exclude_pairs):
        con.register("_excl", exclude_pairs)
        candidates = f"""
            SELECT t.* FROM t
            ANTI JOIN _excl AS e
              ON e."{id_col}" = t."{id_col}" AND e.usrn = t.usrn
        """
    else:
        candidates = "SELECT * FROM t"
    result: pa.Table = con.execute(f"""
        WITH candidates AS (
            {candidates}
        ),
        scored AS (
            SELECT * EXCLUDE (_u_geom, _s_geom),
                {expr} AS overlap_length_pct
            FROM candidates
        ),
        ranked AS (
            SELECT *,
                MAX(overlap_length_pct) OVER (PARTITION BY "{id_col}") AS _max_overlap
            FROM scored
        )
        SELECT * EXCLUDE (_max_overlap)
        FROM ranked
        WHERE _max_overlap >= {min_overlap}
          AND overlap_length_pct >= _max_overlap * 0.8
    """).to_arrow_table()
    log.debug(
        "Phase 2 (corridor) overlap: %d → %d rows kept (best per feature)",
        len(table),
        len(result),
    )
    return result.append_column(
        "match_phase", pa.array([2] * len(result), type=pa.int8())
    )


def _nearest_dedup(table: pa.Table, id_col: str, phase: int = 3) -> pa.Table:
    """Pick the single closest USRN per unmatched RHS feature.

    Shared by the Phase 3 nearest fallback and by Phase 4, where several connected
    neighbours may each offer a USRN and the closest one wins. *phase* stamps the
    resulting ``match_phase`` column.
    """
    if not len(table):
        return table
    con = _duck()
    con.register("t", table)
    # usrn breaks distance ties deterministically. Without it the winner depends on
    # input row order, so an unrelated change to the join plan silently reassigns
    # features between equidistant USRNs.
    result = con.execute(f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY "{id_col}" ORDER BY distance_m, usrn
                ) AS _rn
            FROM t
        )
        SELECT * EXCLUDE (_rn)
        FROM ranked
        WHERE _rn = 1
    """).to_arrow_table()
    return result.append_column(
        "match_phase", pa.array([phase] * len(result), type=pa.int8())
    )


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------


# Rows per batch in the Phase 3 / Phase 4 loops. Small batches keep each sub-query's
# USRN spatial filter tight, so Sedona prunes more USRN row groups.
#
# Phases 1 and 2 deliberately do NOT batch: each runs one query per chunk against the
# chunk-wide envelope. That trades away some row-group pruning for far fewer query
# plans, and measured faster on a national gas_pipe run (17.7s vs 20.7s). The cost is
# that a whole chunk's candidate pairs — corridor WKB included — are materialised in
# one to_arrow_table() before the post-processor sees them.
_ROWS_PER_BATCH = 5_000


def _run_in_batches(
    sd: SedonaContext,
    features: pa.Table,
    rhs_view: str,
    template: str,
    *,
    expand_m: float,
    post_process: Callable[[pa.Table], pa.Table],
    explain: bool,
    label: str,
    indent: str = "",
) -> list[pa.Table]:
    """Run one phase over *features* in ``_ROWS_PER_BATCH`` batches.

    Shared by Phase 3 and Phase 4 in both the national and filtered executors — the
    loop body is identical in all three call sites, so it lives here once. Phases 1
    and 2 run whole-chunk queries instead; see ``_ROWS_PER_BATCH``.

    Each batch gets its own tight USRN spatial filter, derived from that batch's
    envelope grown by *expand_m*, so a USRN just outside the batch bounds is still
    reachable.

    *post_process* is the phase's result reducer — ``_nearest_dedup`` for both current
    callers. Returns one table per non-empty batch.
    """
    parts: list[pa.Table] = []
    n_batches = max(1, (len(features) + _ROWS_PER_BATCH - 1) // _ROWS_PER_BATCH)
    log.info(
        "%s%s: %d unmatched rows → %d batches", indent, label, len(features), n_batches
    )

    for batch_i in range(n_batches):
        batch = features.slice(batch_i * _ROWS_PER_BATCH, _ROWS_PER_BATCH)
        if not len(batch):
            continue

        _register_rhs_view(sd, batch, rhs_view)
        query = fill_spatial_filter(
            template,
            spatial_filter=usrn_spatial_filter(_table_envelope(batch), expand_m),
        )
        if explain and batch_i == 0:
            log_plan(sd, query)

        raw = _normalise_arrow(cast(pa.Table, sd.sql(query).to_arrow_table()))
        log.debug(
            "%s%s batch %d/%d (%d rows): %d candidates",
            indent,
            label,
            batch_i + 1,
            n_batches,
            len(batch),
            len(raw),
        )
        if len(raw):
            reduced = post_process(raw)
            if len(reduced):
                parts.append(reduced)
    return parts


def _phase4_match(
    sd: SedonaContext,
    features: pa.Table,
    matched_parts: list[pa.Table],
    rhs_view: str,
    phase4_template: str,
    *,
    id_col: str,
    tolerance_m: float,
    expand_m: float,
    explain: bool,
    indent: str = "",
) -> list[pa.Table]:
    """Propagate USRNs across physical connections to features Phases 1-3 left unmatched.

    E.g. a gas main that never comes within ``phase3_distance`` of a street is usually a spur
    of a run that does. Rather than reaching for an ever-more-distant street, this
    inherits the USRN of an already-matched feature it physically touches (within
    *tolerance_m*), which is a claim about network membership rather than proximity.

    Returns ``[]`` when disabled (``tolerance_m <= 0``), when nothing is unmatched, or
    when there is no matched feature to seed from.
    """
    if tolerance_m <= 0 or not len(features):
        return []

    matched_parts = [part for part in matched_parts if len(part)]
    if not matched_parts:
        return []
    matched_pairs = pa.concat_tables(matched_parts)

    # Here we pass in features from phase 4
    # the id column we are matching against
    # things from 1,2,3 that have already been matched
    # return the features that haven't been matched yet
    unmatched = features.filter(
        _anti_join_mask(features, id_col, pc.unique(matched_pairs.column(id_col)))
    )
    if not len(unmatched):
        return []

    n_neighbours = _register_neighbours_view(sd, features, matched_pairs, id_col)
    if not n_neighbours:
        return []

    log.info(
        "%sPhase 4 (connected): %d unmatched rows, %d matched neighbours (%.0fm tolerance)",
        indent,
        len(unmatched),
        n_neighbours,
        tolerance_m,
    )
    return _run_in_batches(
        sd,
        unmatched,
        rhs_view,
        phase4_template,
        expand_m=expand_m,
        # Several connected neighbours may each offer a USRN — keep the closest.
        post_process=lambda t: _nearest_dedup(t, id_col, phase=4),
        explain=explain,
        label="Phase 4 (connected)",
        indent=indent,
    )


def _log_line_match_summary(
    total_features: int,
    matched: int,
    p1: int,
    p2: int,
    p3: int,
    p4: int = 0,
    both_p1_p2: int = 0,
) -> None:
    """Log the line-join match rate: how many RHS features matched, broken down by phase.

    *matched* is passed in rather than summed from the phase figures. Phases 1 and 2
    both run over every feature, so their buckets overlap — a line that crosses one
    street's centreline and runs along another's corridor is counted in both. Summing
    would inflate the rate and clamp ``unmatched`` to zero. *both_p1_p2* reports the size
    of that intersection so the breakdown still reconciles.
    """
    unmatched = max(0, total_features - matched)
    rate = (100.0 * matched / total_features) if total_features else 0.0
    log.info(
        "Match summary: %d/%d RHS features matched (%.1f%%) | "
        "Phase 1 (intersect): %d | Phase 2 (corridor): %d (of which %d also Phase 1) | "
        "Phase 3 (nearest): %d | Phase 4 (connected): %d | unmatched: %d",
        matched,
        total_features,
        rate,
        p1,
        p2,
        both_p1_p2,
        p3,
        p4,
        unmatched,
    )


def _assert_corridor_file_current(
    usrn_parquet: pathlib.Path, usrn_line_parquet: pathlib.Path
) -> None:
    """Raise if the corridor file was not built from this USRN file.

    ``prepare-usrns-line`` buffers every row of ``usrns_27700.parquet`` 1:1, so the two
    files must have identical row counts. Re-running ``prepare-usrns`` against a newer
    OS Open USRN release without rebuilding the corridor file breaks that — and the
    failure is silent and wrong rather than loud: Phase 1 inner-joins ``usrns_line`` to
    fetch each USRN's corridor for scoring, so a USRN with no corridor row produces no
    Phase 1 match at all. Those pairs resurface through Phase 2 carrying
    ``is_intersection = false`` and ``match_phase = 2`` — a false statement about a pair
    that genuinely crosses a centreline — and a crossing with low corridor overlap is
    dropped outright by Phase 2's threshold. The filename encodes buffer width, not
    vintage, so a stale file is indistinguishable from a fresh one on disk.

    Row counts come from the parquet footers — metadata only, no geometry is read. A
    differing USRN release always changes the row count, so this catches the realistic
    case without paying to compare the two USRN sets.
    """
    usrn_rows = pq.read_metadata(str(usrn_parquet)).num_rows
    corridor_rows = pq.read_metadata(str(usrn_line_parquet)).num_rows
    if usrn_rows != corridor_rows:
        raise ValueError(
            f"Corridor file is stale: {usrn_line_parquet.name} has {corridor_rows:,} rows "
            f"but {usrn_parquet.name} has {usrn_rows:,}. They must match — "
            "prepare-usrns-line buffers every USRN 1:1.\n"
            "Phase 1 would silently lose every match for a USRN missing from the "
            "corridor file, reporting those pairs as match_phase=2 with "
            "is_intersection=false. Rebuild it:\n"
            "  geo-matcher prepare-usrns-line --buffer-m N --force"
        )


# ---------------------------------------------------------------------------
# Mode executors
# ---------------------------------------------------------------------------


def _national_single_phase(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    query_template: str,
    usrn_expand_m: float,
    explain: bool,
    output_path: pathlib.Path,
    n_chunks: int = 80,  # controlled by NationalMode.n_chunks
    lhs_view_name: str = "usrns",
) -> None:
    """Single-phase NationalMode executor — one LHS file, chunked RHS.

    Used by the ``polygon``/``point`` USRN joins and the UPRN polygon join.
    LHS registered once (as *lhs_view_name*); each RHS chunk's envelope drives
    LHS row-group pruning. Results streamed incrementally to *output_path*.
    """
    sd.read_parquet(str(usrn_parquet)).to_view(lhs_view_name, overwrite=True)

    rhs_pf = pq.ParquetFile(str(rhs_parquet))
    n_rgs = rhs_pf.metadata.num_row_groups
    chunk_row_groups = _split_into_chunks(n_rgs, n_chunks)
    bbox_idx = _bbox_col_indices(rhs_pf)

    log.info(
        "RHS (%s): %d row groups → %d chunks; streaming to %s",
        rhs_view,
        n_rgs,
        len(chunk_row_groups),
        output_path,
    )

    writer: pq.ParquetWriter | None = None
    try:
        for i, chunk_rgs in enumerate(chunk_row_groups):
            chunk = rhs_pf.read_row_groups(chunk_rgs)
            envelope = _row_group_envelope(rhs_pf, chunk_rgs, bbox_idx)

            _register_rhs_view(sd, chunk, rhs_view)

            query = fill_spatial_filter(
                query_template,
                spatial_filter=usrn_spatial_filter(envelope, usrn_expand_m),
            )
            if explain and i == 0:
                log_plan(sd, query)
            result = _normalise_arrow(cast(pa.Table, sd.sql(query).to_arrow_table()))
            if len(result):
                if writer is None:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(str(output_path), result.schema)
                writer.write_table(result)
                log.info(
                    "Chunk %d/%d (%d rhs row groups): %d matches",
                    i + 1,
                    len(chunk_row_groups),
                    len(chunk_rgs),
                    len(result),
                )
    finally:
        if writer:
            writer.close()


def _filtered_single_phase(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    query: str,
    bbox: BBox,
    filter_fn: Callable[[BBox], str],
    explain: bool,
    lhs_view_name: str = "usrns",
) -> pa.Table:
    """Single-phase FilteredMode executor — one query against a city/custom bbox.

    Used by the ``polygon``/``point`` USRN joins and the UPRN polygon join.
    Mirrors ``_national_single_phase`` structurally: unlike NationalMode there's
    no chunking, so both parquets are registered as full Sedona views and the
    query runs once.
    """
    sd.read_parquet(str(usrn_parquet)).to_view(lhs_view_name, overwrite=True)
    sd.read_parquet(str(rhs_parquet)).to_view(rhs_view, overwrite=True)

    usrn_meta = pq.read_metadata(str(usrn_parquet))
    rhs_meta = pq.read_metadata(str(rhs_parquet))
    log.info(
        "%s: %d rows / %d row groups",
        lhs_view_name,
        usrn_meta.num_rows,
        usrn_meta.num_row_groups,
    )
    log.info(
        "RHS (%s): %d rows / %d row groups",
        rhs_view,
        rhs_meta.num_rows,
        rhs_meta.num_row_groups,
    )
    formatted = fill_spatial_filter(query, spatial_filter=filter_fn(bbox))
    if explain:
        log_plan(sd, formatted)
    log.info("Filtered join (usrns × %s)...", rhs_view)
    return cast(pa.Table, sd.sql(formatted).to_arrow_table())


def _national_line_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    intersect_template: str,
    phase2_template: str,
    phase3_template: str,
    phase4_template: str,
    id_col: str,
    distance_m: float,
    explain: bool,
    output_path: pathlib.Path,
    usrn_line_parquet: pathlib.Path,
    n_chunks: int = 80,
    overlap_threshold: float = 0.10,
    phase4_tolerance_m: float = 5.0,
) -> None:
    """Four-phase NationalMode executor for ``line`` joins — two USRN files, chunked RHS.

    Phase 1: ST_Intersects against ``usrns`` (centrelines) over the whole chunk; all
             touching pairs kept.
    Phase 2: ST_Intersects against ``usrns_line`` (buffered corridors), also over the
             **whole chunk** — a line that crosses one street's centreline can still run
             along a neighbouring street's corridor, and gating Phase 2 on Phase 1's
             leftovers made that association unreachable. Pairs Phase 1 already reported
             are excluded so each ``(feature, usrn)`` pair appears once.
    Phase 3: ST_DWithin nearest ``usrns`` for features matched by neither Phase 1 nor
             Phase 2, in batches with the envelope expanded by *distance_m*.
    Phase 4: connectivity inheritance for whatever Phase 3 still left unmatched.

    Results streamed incrementally to *output_path*.
    """
    sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)
    sd.read_parquet(str(usrn_line_parquet)).to_view("usrns_line", overwrite=True)

    rhs_pf = pq.ParquetFile(str(rhs_parquet))
    n_rgs = rhs_pf.metadata.num_row_groups
    chunk_row_groups = _split_into_chunks(n_rgs, n_chunks)
    bbox_idx = _bbox_col_indices(rhs_pf)
    id_type = rhs_pf.schema_arrow.field(id_col).type

    log.info(
        "RHS (%s): %d row groups → %d chunks (four-phase intersect-first); streaming to %s",
        rhs_view,
        n_rgs,
        len(chunk_row_groups),
        output_path,
    )

    # Match-rate accumulators. Each RHS feature lives in exactly one chunk, so summing
    # per-chunk distinct counts gives the global figures. Phases 1 and 2 both see every
    # feature, so their id sets overlap — track the union for the match rate and the
    # intersection for the breakdown.
    total_features = 0
    sum_p1 = sum_p2 = sum_p3 = sum_p4 = 0
    sum_matched = sum_both = 0
    writer: pq.ParquetWriter | None = None
    try:
        for i, chunk_rgs in enumerate(chunk_row_groups):
            log.info(
                "── Chunk %d/%d (%d rhs row groups) ──",
                i + 1,
                len(chunk_row_groups),
                len(chunk_rgs),
            )

            # Accumulated results — one list per phase (reset each chunk)
            phase1_parts: list[pa.Table] = []
            phase2_parts: list[pa.Table] = []
            phase3_parts: list[pa.Table] = []

            # Get the chunk
            chunk = rhs_pf.read_row_groups(chunk_rgs)

            # Calculate its bbox
            # So we can spatially filter
            envelope = _row_group_envelope(rhs_pf, chunk_rgs, bbox_idx)

            _register_rhs_view(sd, chunk, rhs_view)

            # Phase 1: exact chunk envelope (no expansion) — only USRNs that touch the chunk
            log.debug("  Phase 1 (intersect) on %d rows...", len(chunk))
            # Corridor side expanded by distance_m: a buffer reaches that far beyond its
            # centreline, so an unexpanded envelope would prune corridors the join needs.
            intersect_query = fill_spatial_filter(
                intersect_template,
                spatial_filter=usrn_spatial_filter(envelope),
                corridor_filter=usrn_spatial_filter(envelope, distance_m, alias="ul"),
            )
            if explain and i == 0:
                log_plan(sd, intersect_query)
            intersect_result = _phase1_score_overlap(
                _normalise_arrow(
                    cast(pa.Table, sd.sql(intersect_query).to_arrow_table())
                ),
                distance_m,
            )
            log.info("  Phase 1 (intersect): %d matches", len(intersect_result))

            if len(intersect_result):
                phase1_parts.append(intersect_result)

            # Phase 2: the whole chunk again, not Phase 1's leftovers. No envelope
            # expansion — the filter prunes on usrns_line.geometry, the buffered
            # corridor, which already reaches buffer_m past its centreline.
            log.debug("  Phase 2 (corridor) on %d rows...", len(chunk))
            phase2_query = fill_spatial_filter(
                phase2_template, spatial_filter=usrn_spatial_filter(envelope)
            )
            if explain and i == 0:
                log_plan(sd, phase2_query)
            phase2_result = _phase2_select_corridors(
                _normalise_arrow(cast(pa.Table, sd.sql(phase2_query).to_arrow_table())),
                id_col,
                distance_m,
                overlap_threshold,
                exclude_pairs=intersect_result,
            )
            log.info("  Phase 2 (corridor): %d matches", len(phase2_result))
            if len(phase2_result):
                phase2_parts.append(phase2_result)

            # Phase 3: ST_DWithin nearest USRN for features matched by neither phase
            p1_ids = _distinct_ids(phase1_parts, id_col, id_type)
            p2_ids = _distinct_ids(phase2_parts, id_col, id_type)
            phase3_features = chunk.filter(
                _anti_join_mask(chunk, id_col, _union_ids(p1_ids, p2_ids))
            )

            if len(phase3_features):
                # expand_m=distance_m so USRNs just outside the batch bounds stay reachable
                phase3_parts = _run_in_batches(
                    sd,
                    phase3_features,
                    rhs_view,
                    phase3_template,
                    expand_m=distance_m,
                    post_process=lambda t: _nearest_dedup(t, id_col),
                    explain=explain and i == 0,
                    label="Phase 3 (nearest)",
                    indent="  ",
                )

            if phase3_parts:
                log.info(
                    "  Phase 3 (nearest): %d matches",
                    sum(len(t) for t in phase3_parts),
                )

            # Phase 4: inherit a USRN across a physical connection. Confined to this
            # chunk — Hilbert ordering keeps connected features together, so the loss
            # against a global pass is under half a percentage point.
            phase4_parts = _phase4_match(
                sd,
                features=chunk,
                matched_parts=phase1_parts + phase2_parts + phase3_parts,
                rhs_view=rhs_view,
                phase4_template=phase4_template,
                id_col=id_col,
                tolerance_m=phase4_tolerance_m,
                expand_m=distance_m,
                explain=explain and i == 0,
                indent="  ",
            )
            if phase4_parts:
                log.info(
                    "  Phase 4 (connected): %d matches",
                    sum(len(t) for t in phase4_parts),
                )

            # Write all phases for this chunk
            all_parts = phase1_parts + phase2_parts + phase3_parts + phase4_parts
            if all_parts:
                chunk_result = pa.concat_tables(all_parts)
                if writer is None:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(str(output_path), chunk_result.schema)
                writer.write_table(chunk_result)

            # Accumulate match-rate figures (each feature lives in exactly one chunk)
            p3_ids = _distinct_ids(phase3_parts, id_col, id_type)
            p4_ids = _distinct_ids(phase4_parts, id_col, id_type)
            total_features += len(chunk)
            sum_p1 += len(p1_ids)
            sum_p2 += len(p2_ids)
            sum_p3 += len(p3_ids)
            sum_p4 += len(p4_ids)
            sum_both += _intersect_len(p1_ids, p2_ids)
            sum_matched += len(_union_ids(p1_ids, p2_ids, p3_ids, p4_ids))

            log.info(
                "  Chunk %d/%d done: %d intersect + %d corridor + %d nearest + %d connected rows",
                i + 1,
                len(chunk_row_groups),
                sum(len(t) for t in phase1_parts),
                sum(len(t) for t in phase2_parts),
                sum(len(t) for t in phase3_parts),
                sum(len(t) for t in phase4_parts),
            )
    finally:
        if writer:
            writer.close()

    _log_line_match_summary(
        total_features, sum_matched, sum_p1, sum_p2, sum_p3, sum_p4, sum_both
    )


def _filtered_line_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    intersect_template: str,
    phase2_template: str,
    phase3_template: str,
    phase4_template: str,
    id_col: str,
    distance_m: float,
    bbox: BBox,
    explain: bool,
    overlap_threshold: float,
    usrn_line_parquet: pathlib.Path,
    phase4_tolerance_m: float = 5.0,
) -> pa.Table:
    """Four-phase FilteredMode executor for ``line`` joins — two USRN files, city bbox.

    Phase 1 — ``ST_Intersects`` against ``usrns`` with exact bbox prune; all touching
    pairs kept; ``_phase1_score_overlap`` adds ``overlap_length_pct`` and
    ``match_phase=1``.

    Phase 2 — ``ST_Intersects`` against ``usrns_line`` (buffered corridors) over the
    **whole** expanded-bbox feature set, not just Phase 1's leftovers: a line that
    crosses one street's centreline can still run along a neighbouring street's
    corridor, and gating Phase 2 on Phase 1 made that association unreachable.
    ``_phase2_select_corridors`` drops the ``(feature, usrn)`` pairs Phase 1 already
    reported, then keeps the best-corridor USRN(s) per RHS feature (``match_phase=2``).

    Phase 3 — ``ST_DWithin`` against ``usrns`` for features matched by neither Phase 1
    nor Phase 2, in batches. ``_nearest_dedup`` picks the single closest USRN per
    feature (``match_phase=3``).

    Phase 4 — connectivity inheritance for whatever Phase 3 still left unmatched.
    """

    # Accumulated results — one list per phase
    phase1_parts: list[pa.Table] = []
    phase2_parts: list[pa.Table] = []
    phase3_parts: list[pa.Table] = []

    sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)
    sd.read_parquet(str(usrn_line_parquet)).to_view("usrns_line", overwrite=True)

    # Load the RHS once, with the bbox expanded so a feature just outside the city
    # bounds can still reach a corridor inside them. All four phases work from this set.
    #
    # It is registered as an in-memory table rather than a parquet view on purpose.
    # Handing Sedona `read_parquet(rhs).to_view(...)` and letting the WHERE clause prune
    # gives the planner no post-filter cardinality, and combined with Phase 1's
    # `usrns_line` equijoin it picks a join order that does not terminate in any
    # reasonable time: on gas_pipe (2.27M rows) a 56 km² bbox ran >100s against 0.4s
    # for the identical query over an in-memory table, for byte-identical output.
    # Small RHS files hide it — ngn_mains (113k rows) finishes either way. This also
    # makes the filtered path consistent with the national one, which has always
    # materialised each chunk before querying it.
    xmin, ymin, xmax, ymax = bbox
    ex = distance_m
    all_features: pa.Table = pq.read_table(
        str(rhs_parquet),
        filters=(
            (pc.field("bbox", "xmax") >= xmin - ex)
            & (pc.field("bbox", "xmin") <= xmax + ex)
            & (pc.field("bbox", "ymax") >= ymin - ex)
            & (pc.field("bbox", "ymin") <= ymax + ex)
        ),
    )
    log.info("RHS features in scope: %d rows (expanded bbox)", len(all_features))
    # The "started with" denominator for the match-rate summary.
    total_features = len(all_features)
    id_type = all_features.column(id_col).type
    if len(all_features):
        _register_rhs_view(sd, all_features, rhs_view)

    # Phase 1: exact ST_Intersects with standard bbox prune (no expansion). The RHS is
    # the expanded-bbox table, so bbox_pruner's `s` predicate still does the work of
    # narrowing it back to the exact bbox — Phase 1's result is unchanged by the switch.
    intersect_result: pa.Table = pa.table({})
    if len(all_features):
        intersect_query = fill_spatial_filter(
            intersect_template,
            spatial_filter=bbox_pruner(bbox),
            corridor_filter=usrn_spatial_filter(bbox, distance_m, alias="ul"),
        )
        log.debug("Phase 1 query:\n%s", intersect_query)

        if explain:
            log_plan(sd, intersect_query)
        intersect_result = _phase1_score_overlap(
            _normalise_arrow(cast(pa.Table, sd.sql(intersect_query).to_arrow_table())),
            distance_m,
        )
    log.info("Phase 1 (intersect): %d matches", len(intersect_result))

    # If there's stuff to collect put it in phase1 list
    if len(intersect_result):
        phase1_parts.append(intersect_result)

    if len(all_features):
        # One query over every feature. No envelope expansion — the filter prunes on
        # usrns_line.geometry, the buffered corridor, which already reaches buffer_m
        # past its centreline.
        phase2_query = fill_spatial_filter(
            phase2_template,
            spatial_filter=usrn_spatial_filter(_table_envelope(all_features)),
        )
        if explain:
            log_plan(sd, phase2_query)
        phase2_result = _phase2_select_corridors(
            _normalise_arrow(cast(pa.Table, sd.sql(phase2_query).to_arrow_table())),
            id_col,
            distance_m,
            overlap_threshold,
            exclude_pairs=intersect_result,
        )
        log.info("Phase 2 (corridor): %d matches", len(phase2_result))
        if len(phase2_result):
            phase2_parts.append(phase2_result)

    # Phase 3 gets whatever neither Phase 1 nor Phase 2 matched
    p1_ids = _distinct_ids(phase1_parts, id_col, id_type)
    p2_ids = _distinct_ids(phase2_parts, id_col, id_type)
    phase3_features = all_features.filter(
        _anti_join_mask(all_features, id_col, _union_ids(p1_ids, p2_ids))
    )

    if len(phase3_features):
        # expand_m=distance_m matches _national_line_join — without it a USRN within
        # phase3_distance of a feature but outside the batch bounds is missed.
        phase3_parts = _run_in_batches(
            sd,
            phase3_features,
            rhs_view,
            phase3_template,
            expand_m=distance_m,
            post_process=lambda t: _nearest_dedup(t, id_col),
            explain=explain,
            label="Phase 3 (nearest)",
        )

    if phase3_parts:
        log.info("Phase 3 (nearest): %d matches", sum(len(t) for t in phase3_parts))

    # Phase 4: inherit a USRN across a physical connection to an already-matched feature
    phase4_parts = _phase4_match(
        sd,
        all_features,
        phase1_parts + phase2_parts + phase3_parts,
        rhs_view,
        phase4_template,
        id_col=id_col,
        tolerance_m=phase4_tolerance_m,
        expand_m=distance_m,
        explain=explain,
    )
    if phase4_parts:
        log.info(
            "Phase 4 (connected): %d matches",
            sum(len(t) for t in phase4_parts),
        )

    p3_ids = _distinct_ids(phase3_parts, id_col, id_type)
    p4_ids = _distinct_ids(phase4_parts, id_col, id_type)
    _log_line_match_summary(
        total_features,
        len(_union_ids(p1_ids, p2_ids, p3_ids, p4_ids)),
        len(p1_ids),
        len(p2_ids),
        len(p3_ids),
        len(p4_ids),
        _intersect_len(p1_ids, p2_ids),
    )

    all_parts = phase1_parts + phase2_parts + phase3_parts + phase4_parts
    return pa.concat_tables(all_parts) if all_parts else pa.table({})


# ---------------------------------------------------------------------------
# Dispatchers
# ---------------------------------------------------------------------------


# TODO: line joins still have their own dispatch pair (_filtered_line_join /
# _national_line_join) rather than sharing this dispatcher — unifying single-phase
# and four-phase dispatch remains a bigger change than the FilteredMode/NationalMode
# symmetry fix below.
def execute_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    query: str,
    mode: JoinMode,
    filter_fn: Callable[[BBox], str],
    usrn_expand_m: float = 0.0,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    lhs_view_name: str = "usrns",
) -> pa.Table:
    """Single-phase dispatcher for the ``polygon``/``point`` USRN joins and the UPRN
    polygon join.

    Line joins go through ``execute_line_join`` instead.

    FilteredMode: delegates to ``_filtered_single_phase`` — one query against the bbox.

    NationalMode: delegates to ``_national_single_phase`` which chunks the RHS and streams
    results. ``usrn_expand_m`` expands the LHS spatial filter per chunk (= ``distance_m``
    for point joins so LHS rows just outside the raw chunk envelope remain reachable).

    ``lhs_view_name`` names the Sedona view *usrn_parquet* is registered under —
    ``"usrns"`` by default (every USRN join), ``"uprns"`` for the UPRN join —
    the *query* text must reference whichever name it's given.
    """
    match mode:
        case FilteredMode(bbox=bbox):
            return _filtered_single_phase(
                sd,
                usrn_parquet,
                rhs_parquet,
                rhs_view,
                query,
                bbox,
                filter_fn,
                explain,
                lhs_view_name=lhs_view_name,
            )

        case NationalMode(n_chunks=n_chunks):
            if output_path is not None:
                _national_single_phase(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    query,
                    usrn_expand_m,
                    explain,
                    output_path,
                    n_chunks=n_chunks,
                    lhs_view_name=lhs_view_name,
                )
                return pa.table({})
            with tempfile.TemporaryDirectory() as _tmp:
                _path = pathlib.Path(_tmp) / "stream.parquet"
                _national_single_phase(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    query,
                    usrn_expand_m,
                    explain,
                    _path,
                    n_chunks=n_chunks,
                    lhs_view_name=lhs_view_name,
                )
                return pq.read_table(str(_path)) if _path.exists() else pa.table({})


def execute_line_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    intersect_template: str,
    phase2_template: str,
    phase3_template: str,
    phase4_template: str,
    id_col: str,
    distance_m: float,
    mode: JoinMode,
    usrn_line_parquet: pathlib.Path,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    overlap_threshold: float = 0.10,
    phase4_tolerance_m: float = 5.0,
) -> pa.Table:
    """Four-phase dispatcher for ``line`` joins — always uses two USRN parquets.

    FilteredMode: delegates to ``_filtered_line_join``.
    NationalMode: delegates to ``_national_line_join``.

    Both paths depend on the corridor file matching the USRN file, so that is checked
    once here before any work starts.
    """
    _assert_corridor_file_current(usrn_parquet, usrn_line_parquet)

    match mode:
        case FilteredMode(bbox=bbox):
            return _filtered_line_join(
                sd,
                usrn_parquet,
                rhs_parquet,
                rhs_view,
                intersect_template,
                phase2_template,
                phase3_template,
                phase4_template,
                id_col,
                distance_m,
                bbox,
                explain,
                overlap_threshold,
                usrn_line_parquet,
                phase4_tolerance_m=phase4_tolerance_m,
            )

        case NationalMode(n_chunks=n_chunks):
            if output_path is not None:
                _national_line_join(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    intersect_template,
                    phase2_template,
                    phase3_template,
                    phase4_template,
                    id_col,
                    distance_m,
                    explain,
                    output_path,
                    usrn_line_parquet,
                    n_chunks=n_chunks,
                    overlap_threshold=overlap_threshold,
                    phase4_tolerance_m=phase4_tolerance_m,
                )
                return pa.table({})
            with tempfile.TemporaryDirectory() as _tmp:
                _path = pathlib.Path(_tmp) / "stream.parquet"
                _national_line_join(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    intersect_template,
                    phase2_template,
                    phase3_template,
                    phase4_template,
                    id_col,
                    distance_m,
                    explain=explain,
                    output_path=_path,
                    n_chunks=n_chunks,
                    overlap_threshold=overlap_threshold,
                    usrn_line_parquet=usrn_line_parquet,
                    phase4_tolerance_m=phase4_tolerance_m,
                )
                return pq.read_table(str(_path)) if _path.exists() else pa.table({})


# ---------------------------------------------------------------------------
# Join strategies
#
# Registered under (LhsKind, GeometryType) — three USRN strategies plus one
# UPRN strategy so far, three architectures:
#
#   (usrn, polygon)  — run_usrn_polygon_join — polygon datasets (e.g. soil, land cover)
#              Single-phase · 1 USRN file (usrns_27700.parquet)
#              ST_Intersects(u.geometry, s.geometry)
#
#   (usrn, point)    — run_usrn_point_join — point datasets (e.g. bus stops, traffic counts)
#              Single-phase · 1 USRN file (usrns_27700.parquet)
#              ST_DWithin(u.geometry, s.geometry, distance_m)
#
#   (usrn, line)     — run_usrn_line_join — linestring datasets (e.g. gas pipes, cables)
#              Four-phase · 2 USRN files (usrns_27700.parquet + usrns_line_Nm_27700.parquet)
#              Phase 1: ST_Intersects against centrelines (all touching pairs kept)
#              Phase 2: ST_Intersects against pre-buffered corridor polygons, over the
#                       same full feature set as Phase 1 (overlap-filtered; Phase 1's
#                       own pairs excluded). Phases 1 and 2 are unioned, not chained.
#              Phase 3: ST_DWithin nearest USRN for features matched by neither
#              Phase 4: inherit a USRN from a physically connected matched feature
#
#   (uprn, polygon)  — run_uprn_polygon_join — address points against polygon
#              datasets (e.g. wards, flood zones)
#              Single-phase · 1 UPRN file (uprns_27700.parquet)
#              ST_Intersects(u.geometry, s.geometry) — reuses execute_join /
#              _national_single_phase / _filtered_single_phase exactly as the
#              USRN polygon join does, just registering "uprns" instead of
#              "usrns" as the LHS view (lhs_view_name="uprns").
# ---------------------------------------------------------------------------


@register(LhsKind.USRN, GeometryType.POLYGON)
def run_usrn_polygon_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: JoinMode = _DEFAULT_MODE,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    **_kwargs: Any,
) -> pa.Table:
    """Intersect USRNs against a polygon dataset — single-phase, one USRN file."""
    rhs_view: str = rhs_config.name

    match mode:
        case FilteredMode(bbox=bbox):
            log.info("Bbox filter: xmin=%s ymin=%s xmax=%s ymax=%s", *bbox)
        case NationalMode():
            log.info("No bbox supplied — running full national join.")

    cols_fragment: str = col_fragment(rhs_config)

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {cols_fragment}
        FROM usrns AS u
        JOIN {rhs_view} AS s
          ON ST_Intersects(u.geometry, s.geometry)
        WHERE TRUE
        {{spatial_filter}}
        ORDER BY u.usrn
    """

    return execute_join(
        sd,
        usrn_parquet,
        rhs_config.parquet_path,
        rhs_view,
        query,
        mode,
        filter_fn=bbox_pruner,
        explain=explain,
        output_path=output_path,
        usrn_expand_m=0.0,
    )


@register(LhsKind.UPRN, GeometryType.POLYGON)
def run_uprn_polygon_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,  # holds the UPRN parquet path — see module docstring note below
    rhs_config: DatasetConfig,
    *,
    mode: JoinMode = _DEFAULT_MODE,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    **_kwargs: Any,
) -> pa.Table:
    """Intersect UPRN address points against a polygon dataset — single-phase, one UPRN file.

    Structurally identical to ``run_usrn_polygon_join`` — same ``execute_join``
    engine, same ``ST_Intersects`` predicate — just registering the LHS view as
    ``"uprns"`` instead of ``"usrns"`` (``lhs_view_name="uprns"``) and selecting
    ``uprn`` instead of ``usrn``/``street_type`` (the prepared UPRN GeoParquet
    carries no ``street_type`` equivalent).

    The parameter is named ``usrn_parquet`` (not ``uprn_parquet``) so this
    function is callable through ``GeoMatcher.match_dispatch`` unchanged — that
    call site always passes ``usrn_parquet=self._usrn_parquet`` regardless of
    which LHS kind was requested. Renaming that concept throughout is deferred
    to the wider project rename mentioned separately.
    """
    rhs_view: str = rhs_config.name

    match mode:
        case FilteredMode(bbox=bbox):
            log.info("Bbox filter: xmin=%s ymin=%s xmax=%s ymax=%s", *bbox)
        case NationalMode():
            log.info("No bbox supplied — running full national join.")

    cols_fragment: str = col_fragment(rhs_config)

    query: str = f"""
        SELECT
            u.uprn
            {cols_fragment}
        FROM uprns AS u
        JOIN {rhs_view} AS s
          ON ST_Intersects(u.geometry, s.geometry)
        WHERE TRUE
        {{spatial_filter}}
        ORDER BY u.uprn
    """

    return execute_join(
        sd,
        usrn_parquet,
        rhs_config.parquet_path,
        rhs_view,
        query,
        mode,
        filter_fn=bbox_pruner,
        explain=explain,
        output_path=output_path,
        usrn_expand_m=0.0,
        lhs_view_name="uprns",
    )


@register(LhsKind.USRN, GeometryType.POINT)
def run_usrn_point_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: JoinMode = _DEFAULT_MODE,
    distance_m: float = 10.0,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    **_kwargs: Any,
) -> pa.Table:
    """Assign each point to its nearest USRN — single-phase, one USRN file.

    ST_DWithin finds all USRNs within ``distance_m`` metres, ordered by distance.
    """
    rhs_view: str = rhs_config.name

    match mode:
        case FilteredMode(bbox=bbox):
            log.info(
                "Bbox filter (USRNs exact, points expanded by %.0fm): "
                "xmin=%s ymin=%s xmax=%s ymax=%s",
                distance_m,
                *bbox,
            )
        case NationalMode():
            log.info("No bbox supplied — matching all points.")

    cols_fragment: str = col_fragment(rhs_config)

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {cols_fragment},
            ST_Distance(u.geometry, s.geometry) AS distance_m
        FROM usrns AS u
        JOIN {rhs_view} AS s
          ON ST_DWithin(u.geometry, s.geometry, {distance_m})
        WHERE TRUE
        {{spatial_filter}}
        ORDER BY u.usrn, distance_m
    """

    return execute_join(
        sd,
        usrn_parquet,
        rhs_config.parquet_path,
        rhs_view,
        query,
        mode,
        filter_fn=lambda bbox: bbox_nearest_filters(bbox, distance_m),
        explain=explain,
        output_path=output_path,
        usrn_expand_m=distance_m,
    )


@register(LhsKind.USRN, GeometryType.LINE)
def run_usrn_line_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: JoinMode = _DEFAULT_MODE,
    distance_m: float = 10.0,
    phase3_distance_m: float | None = None,
    explain: bool = False,
    rhs_id_col: str | None = None,
    output_path: pathlib.Path | None = None,
    overlap_threshold: float = 0.10,
    usrn_line_parquet: pathlib.Path | None = None,
    phase4_tolerance_m: float = 5.0,
    **_kwargs: Any,
) -> pa.Table:
    """Match each linestring in the RHS dataset to USRNs — always four-phase, two USRN files always needed.

    Phase 1 uses ``usrns_27700.parquet`` (centrelines) with ``ST_Intersects`` —
    touching pairs are definitive matches (``match_phase=1``).

    Phase 2 uses ``usrns_line_Nm_27700.parquet`` (pre-buffered corridor polygons) with
    ``ST_Intersects`` over the **same full feature set** as Phase 1, not just its
    leftovers: a line that crosses one street can still run the length of another, and
    the two phases answer different questions. ``_phase2_select_corridors`` drops the
    ``(feature, usrn)`` pairs Phase 1 already reported, then selects the USRN(s) whose
    corridor best covers each line (``match_phase=2``).

    Phase 3 uses ``usrns_27700.parquet`` with ``ST_DWithin`` for features matched by
    neither Phase 1 nor Phase 2. The single closest USRN per feature is kept
    (``match_phase=3``). ``overlap_length_pct`` is ``0.0`` for Phase 3 rows.

    Phase 4 gives whatever is still unmatched the USRN of a physically connected feature
    within ``phase4_tolerance_m`` (``match_phase=4``). Disabled at tolerance 0.

    ``rhs_id_col`` is required to track which RHS features are matched between phases.
    """
    if not rhs_id_col:
        raise ValueError(
            "run_usrn_line_join requires --rhs-id-col to track matched RHS features between phases."
        )
    # Validated once here, at the point rhs_id_col first enters the pipeline — every
    # downstream phase (_phase2_select_corridors, _nearest_dedup, _register_neighbours_view)
    # threads the same already-validated value through rather than re-checking it.
    rhs_id_col = sql_ident(rhs_id_col, context="--rhs-id-col")
    if usrn_line_parquet is None:
        raise ValueError(
            "run_usrn_line_join requires --usrn-line-parquet. "
            "Run 'geo-matcher prepare-usrns-line --buffer-m N' first."
        )

    rhs_view: str = rhs_config.name
    log.debug(f"Right handside dataset name is: {rhs_view}")

    match mode:
        case FilteredMode(bbox=bbox):
            log.info(
                "Bbox filter four-phase (USRNs exact, RHS expanded by %.0fm): xmin=%s ymin=%s xmax=%s ymax=%s",
                distance_m,
                *bbox,
            )
        case NationalMode():
            log.info("No bbox supplied — matching all lines (four-phase).")

    # These are the non geometry columns from the RHS dataset
    # We build this once for all join phases
    cols_fragment: str = col_fragment(rhs_config)

    # The match itself is against the raw centreline (u.geometry). ``usrns_line`` is
    # joined on usrn only to fetch that USRN's pre-buffered corridor for the overlap
    # score — see OVERLAP_EXPR_CORRIDOR for why this beats buffering per row.
    intersect_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {cols_fragment},
                ST_Distance(u.geometry, s.geometry) AS distance_m,
                TRUE AS is_intersection,
                ST_AsWKB(ul.geometry) AS _u_geom,
                ST_AsWKB(s.geometry) AS _s_geom
            FROM usrns AS u
            JOIN {rhs_view} AS s ON ST_Intersects(u.geometry, s.geometry)
            JOIN usrns_line AS ul ON ul.usrn = u.usrn
            WHERE TRUE
            {{spatial_filter}}
            {{corridor_filter}}
            ORDER BY u.usrn, distance_m
        """
    # Phase 2 scores overlap against the pre-buffered corridor (u.geometry) — the same
    # geometry the join matched on — so _u_geom is the corridor WKB and the overlap
    # filter intersects it directly (no per-row ST_Buffer). distance_m still measures to
    # the centreline (u.geometry_line).
    phase2_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {cols_fragment},
                ST_Distance(u.geometry_line, s.geometry) AS distance_m,
                FALSE AS is_intersection,
                ST_AsWKB(u.geometry) AS _u_geom,
                ST_AsWKB(s.geometry) AS _s_geom
            FROM usrns_line AS u
            JOIN {rhs_view} AS s ON ST_Intersects(u.geometry, s.geometry)
            WHERE TRUE
            {{spatial_filter}}
            ORDER BY u.usrn, distance_m
        """
    phase3_distance: float = (
        phase3_distance_m if phase3_distance_m is not None else distance_m
    )
    phase3_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {cols_fragment},
                ST_Distance(u.geometry, s.geometry) AS distance_m,
                FALSE AS is_intersection,
                CAST(0.0 AS DOUBLE) AS overlap_length_pct
            FROM usrns AS u
            JOIN {rhs_view} AS s ON ST_DWithin(u.geometry, s.geometry, {phase3_distance})
            WHERE TRUE
            {{spatial_filter}}
            ORDER BY u.usrn, distance_m
        """
    # Phase 4 propagates a USRN across a physical connection rather than measuring
    # proximity to a street: an unmatched feature within phase4_tolerance_m of an
    # already-matched feature inherits that feature's USRN. ``distance_m`` is still the
    # true distance from this feature to the inherited USRN centreline, so a consumer
    # can see how far the attribution reaches; it is typically well beyond
    # phase3_distance, which is exactly why these rows are flagged match_phase=4.
    phase4_template: str = f"""
            SELECT
                n.usrn,
                n.street_type
                {cols_fragment},
                ST_Distance(u.geometry, s.geometry) AS distance_m,
                FALSE AS is_intersection,
                CAST(0.0 AS DOUBLE) AS overlap_length_pct
            FROM neighbours AS n
            JOIN {rhs_view} AS s
              ON ST_DWithin(n.geometry, s.geometry, {phase4_tolerance_m})
            JOIN usrns AS u ON u.usrn = n.usrn
            WHERE TRUE
            {{spatial_filter}}
            ORDER BY n.usrn, distance_m
        """

    return execute_line_join(
        sd,
        usrn_parquet,
        rhs_config.parquet_path,
        rhs_view,
        intersect_template=intersect_template,
        phase2_template=phase2_template,
        phase3_template=phase3_template,
        phase4_template=phase4_template,
        id_col=rhs_id_col,
        distance_m=distance_m,
        mode=mode,
        usrn_line_parquet=usrn_line_parquet,
        explain=explain,
        output_path=output_path,
        overlap_threshold=overlap_threshold,
        phase4_tolerance_m=phase4_tolerance_m,
    )
