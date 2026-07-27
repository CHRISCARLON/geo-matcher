import functools
import logging
import pathlib
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar, cast, runtime_checkable

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sedonadb.context import SedonaContext

from .config import BBox, DatasetConfig, GeometryType
from .explain import log_plan
from .logger import get_logger

log: logging.Logger = get_logger()


# ---------------------------------------------------------------------------
# Analysis mode
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
    """Full national join — RHS split into *n_chunks* spatial slices processed one at a time."""

    n_chunks: int = 50


AnalysisMode = FilteredMode | NationalMode

_DEFAULT_MODE: AnalysisMode = NationalMode()


# ---------------------------------------------------------------------------
# Registry
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
        mode: AnalysisMode = ...,
        explain: bool = ...,
        distance_m: float = ...,
        phase3_distance_m: float | None = ...,
        rhs_id_col: str | None = ...,
        output_path: pathlib.Path | None = ...,
        overlap_threshold: float = ...,
        usrn_line_parquet: pathlib.Path | None = ...,
        phase4_tolerance_m: float = ...,
    ) -> pa.Table: ...


# The registry dictionary that get_join reads from — keyed by GeometryType so the
# registered join strategies can never drift from the enum.
_registry: dict[GeometryType, JoinFn] = {}
_J = TypeVar("_J", bound=JoinFn)


def register(name: GeometryType | str) -> Callable[[_J], _J]:
    """Register a join function under the *name* geometry type.

    Raises at import time if *name* is not a GeometryType, so a typo in a
    @register decorator can never produce an unreachable join.
    """
    try:
        geometry_type: GeometryType = GeometryType(name)
    except ValueError:
        raise ValueError(
            f"Cannot register join {name!r}: not a GeometryType. "
            f"Available: {[g.value for g in GeometryType]}"
        ) from None

    # The inner function that registers the function against
    # what is in @register(GeometryType.POINT)
    def decorator(fn: _J) -> _J:
        _registry[geometry_type] = fn
        return fn

    return decorator


def get_join(name: GeometryType | str) -> JoinFn:
    """Look up the join registered for a geometry type.

    Accepts either a GeometryType member or its plain-string value — StrEnum
    members hash and compare equal to their values.
    """
    if name not in _registry:
        raise KeyError(
            f"No join registered for '{name}'. "
            f"Available: {[g.value for g in _registry]}"
        )
    return _registry[GeometryType(name)]


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


# ---------------------------------------------------------------------------
# Row-group batch helpers
# ---------------------------------------------------------------------------


def _bbox_col_indices(pf: pq.ParquetFile) -> dict[str, int]:
    """Map bbox sub-field name → column index in the parquet row-group metadata."""
    first_rg = pf.metadata.row_group(0)
    path_to_idx = {
        first_rg.column(i).path_in_schema: i for i in range(first_rg.num_columns)
    }
    log.debug("_bbox_col_indices")
    log.debug(first_rg)
    log.debug(path_to_idx)
    return {f: path_to_idx[f"bbox.{f}"] for f in ("xmin", "ymin", "xmax", "ymax")}


def _slice_envelope(
    pf: pq.ParquetFile, rg_slice: list[int], bbox_idx: dict[str, int]
) -> BBox:
    """Derive the spatial envelope of a set of row groups from their column statistics."""
    rgs = [pf.metadata.row_group(i) for i in rg_slice]
    log.debug("_slice_envelope")
    log.debug(rgs)
    return (
        min(rg.column(bbox_idx["xmin"]).statistics.min for rg in rgs),
        min(rg.column(bbox_idx["ymin"]).statistics.min for rg in rgs),
        max(rg.column(bbox_idx["xmax"]).statistics.max for rg in rgs),
        max(rg.column(bbox_idx["ymax"]).statistics.max for rg in rgs),
    )


# Rows per sub-batch in the Phase 2 / Phase 3 loops. Small batches keep each
# sub-query's USRN spatial filter tight, so Sedona prunes more row groups.
_ROWS_PER_BATCH = 5_000


def _bbox_to_wkt(bbox: BBox, expand: float = 0.0) -> str:
    """Return a closed POLYGON WKT for *bbox*, grown by *expand* metres on every side."""
    xmin, ymin, xmax, ymax = bbox
    x0, y0, x1, y1 = xmin - expand, ymin - expand, xmax + expand, ymax + expand
    return f"POLYGON(({x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))"


def _usrn_spatial_filter(bbox: BBox, expand: float = 0.0, alias: str = "u") -> str:
    """Return the ``AND ST_Intersects(...)`` fragment that prunes a USRN-side scan.

    *alias* selects which relation to prune — ``u`` for the centreline table, ``ul``
    for the buffered corridor table that Phase 1 joins for overlap scoring.
    """
    wkt = _bbox_to_wkt(bbox, expand)
    return (
        f"AND ST_Intersects({alias}.geometry, "
        f"ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"
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


def _run_sub_batches(
    sd: SedonaContext,
    rhs_table: pa.Table,
    rhs_view: str,
    template: str,
    *,
    expand_m: float,
    post_process: Callable[[pa.Table], pa.Table],
    explain: bool,
    label: str,
    indent: str = "",
) -> list[pa.Table]:
    """Run one phase over *rhs_table* in ``_ROWS_PER_BATCH`` slices.

    Shared by Phase 2 and Phase 3 in both the national and filtered executors — the
    loop body is identical in all four cases, so it lives here once.

    Each slice gets its own tight USRN spatial filter derived from that slice's envelope, grown by
    *expand_m* (0 for corridor matching, where the buffered geometry already covers the
    reach; ``max_d`` for the nearest fallback, so USRNs just outside the slice bounds
    remain reachable).

    *post_process* is the phase's result reducer (``_overlap_post_filter`` for Phase 2,
    ``_phase3_nearest_dedup`` for Phase 3). Returns one table per non-empty batch.
    """
    parts: list[pa.Table] = []
    n_batches = max(1, (len(rhs_table) + _ROWS_PER_BATCH - 1) // _ROWS_PER_BATCH)
    log.info(
        "%s%s: %d unmatched rows → %d batches", indent, label, len(rhs_table), n_batches
    )

    for batch_i in range(n_batches):
        sub = rhs_table.slice(batch_i * _ROWS_PER_BATCH, _ROWS_PER_BATCH)
        if not len(sub):
            continue

        _register_rhs_view(sd, sub, rhs_view)
        query = template.format(
            spatial_filter=_usrn_spatial_filter(_table_envelope(sub), expand_m)
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
            len(sub),
            len(raw),
        )
        if len(raw):
            reduced = post_process(raw)
            if len(reduced):
                parts.append(reduced)
    return parts


# ---------------------------------------------------------------------------
# Schema normalisation
# ---------------------------------------------------------------------------


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
    table: pa.Table, id_col: str, exclude_ids: set
) -> pa.Array | pa.ChunkedArray:
    """Boolean mask — True where ``table[id_col]`` is NOT in ``exclude_ids``.

    Vectorised replacement for per-row Python ``[v not in s for v in col.to_pylist()]``
    pattern: ``pc.is_in`` runs in Arrow's C++ instead of the Python interpreter.
    An empty ``exclude_ids`` keeps every row (all-True mask).
    """
    col = table.column(id_col)
    if not exclude_ids:
        return pa.array([True] * len(table), type=pa.bool_())
    value_set = pa.array(list(exclude_ids), type=col.type)
    return pc.invert(pc.is_in(col, value_set=value_set))


def _distinct_count(parts: list[pa.Table], id_col: str) -> int:
    """Number of distinct ``id_col`` values across a list of result tables."""
    ids: set = set()
    for t in parts:
        ids.update(t.column(id_col).to_pylist())
    return len(ids)


def _log_line_match_summary(
    total_features: int, p1: int, p2: int, p3: int, p4: int = 0
) -> None:
    """Log the line-join match rate: how many RHS features matched, broken down by phase."""
    matched = p1 + p2 + p3 + p4
    unmatched = max(0, total_features - matched)
    rate = (100.0 * matched / total_features) if total_features else 0.0
    log.info(
        "Match summary: %d/%d RHS features matched (%.1f%%) | "
        "Phase 1 (intersect): %d | Phase 2 (corridor): %d | Phase 3 (nearest): %d | "
        "Phase 4 (connected): %d | unmatched: %d",
        matched,
        total_features,
        rate,
        p1,
        p2,
        p3,
        p4,
        unmatched,
    )


def _propagate_phase4(
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

    A gas main that never comes within ``phase3_distance`` of a street is usually a spur
    of a run that does. Rather than reaching for an ever-more-distant street, this
    inherits the USRN of an already-matched feature it physically touches (within
    *tolerance_m*), which is a claim about network membership rather than proximity.

    Returns ``[]`` when disabled (``tolerance_m <= 0``), when nothing is unmatched, or
    when there is no matched feature to seed from.
    """
    if tolerance_m <= 0 or not len(features):
        return []

    matched_parts = [t for t in matched_parts if len(t)]
    if not matched_parts:
        return []
    matched = pa.concat_tables(matched_parts)

    unmatched = features.filter(
        _anti_join_mask(features, id_col, set(matched.column(id_col).to_pylist()))
    )
    if not len(unmatched):
        return []

    n_neighbours = _register_neighbours_view(sd, features, matched, id_col)
    if not n_neighbours:
        return []

    log.info(
        "%sPhase 4 (connected): %d unmatched rows, %d matched neighbours (%.0fm tolerance)",
        indent,
        len(unmatched),
        n_neighbours,
        tolerance_m,
    )
    return _run_sub_batches(
        sd,
        unmatched,
        rhs_view,
        phase4_template,
        expand_m=expand_m,
        # Several connected neighbours may each offer a USRN — keep the closest.
        post_process=lambda t: _phase3_nearest_dedup(t, id_col, phase=4),
        explain=explain,
        label="Phase 4 (connected)",
        indent=indent,
    )


# ---------------------------------------------------------------------------
# Core executor
# ---------------------------------------------------------------------------


# TODO: bring into a single dispatch method again
# We need to bring the line joins into this too
def execute_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    query: str,
    mode: AnalysisMode,
    filter_fn: Callable[[BBox], str],
    usrn_expand_m: float = 0.0,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
) -> pa.Table:
    """Single-phase dispatcher for ``polygon`` and ``point`` joins.

    FilteredMode: both parquets registered as Sedona views; query is ran directly — logic
    inlined here, no helper function.

    NationalMode: delegates to ``_national_single_phase`` which chunks the RHS and streams
    results. ``usrn_expand_m`` expands the USRN spatial filter per chunk (= ``distance_m``
    for point joins so USRNs just outside the raw chunk envelope remain reachable).
    """
    rhs_meta = pq.read_metadata(str(rhs_parquet))

    match mode:
        case FilteredMode(bbox=bbox):
            # Create the views here
            sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)
            sd.read_parquet(str(rhs_parquet)).to_view(rhs_view, overwrite=True)

            usrn_meta = pq.read_metadata(str(usrn_parquet))
            log.info(
                "USRN: %d rows / %d row groups",
                usrn_meta.num_rows,
                usrn_meta.num_row_groups,
            )
            log.info(
                "RHS (%s): %d rows / %d row groups",
                rhs_view,
                rhs_meta.num_rows,
                rhs_meta.num_row_groups,
            )
            spatial_filter = filter_fn(bbox)
            formatted = query.format(spatial_filter=spatial_filter)
            if explain:
                log_plan(sd, formatted)
            log.info("Filtered join (usrns × %s)...", rhs_view)
            return cast(pa.Table, sd.sql(formatted).to_arrow_table())

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
    max_d: float,
    mode: AnalysisMode,
    usrn_line_parquet: pathlib.Path,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    overlap_threshold: float = 0.10,
    phase4_tolerance_m: float = 5.0,
) -> pa.Table:
    """Four-phase dispatcher for ``line`` joins — always uses two USRN parquets.

    FilteredMode: delegates to ``_filtered_three_phase``.
    NationalMode: delegates to ``_national_three_phase``.
    """
    match mode:
        case FilteredMode(bbox=bbox):
            return _filtered_three_phase(
                sd,
                usrn_parquet,
                rhs_parquet,
                rhs_view,
                intersect_template,
                phase2_template,
                phase3_template,
                phase4_template,
                id_col,
                max_d,
                bbox,
                explain,
                overlap_threshold,
                usrn_line_parquet,
                phase4_tolerance_m=phase4_tolerance_m,
            )

        case NationalMode(n_chunks=n_chunks):
            if output_path is not None:
                _national_three_phase(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    intersect_template,
                    phase2_template,
                    phase3_template,
                    phase4_template,
                    id_col,
                    max_d,
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
                _national_three_phase(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    intersect_template,
                    phase2_template,
                    phase3_template,
                    phase4_template,
                    id_col,
                    max_d,
                    explain=explain,
                    output_path=_path,
                    n_chunks=n_chunks,
                    overlap_threshold=overlap_threshold,
                    usrn_line_parquet=usrn_line_parquet,
                    phase4_tolerance_m=phase4_tolerance_m,
                )
                return pq.read_table(str(_path)) if _path.exists() else pa.table({})


# ---------------------------------------------------------------------------
# Phased executors
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
    n_chunks: int = 50,  # controlled by NationalMode.n_chunks
) -> None:
    """Single-phase NationalMode executor — one USRN file, chunked RHS.

    Used by ``polygon`` and ``point`` joins.
    USRN registered once; each RHS chunk's envelope drives USRN row-group pruning.
    Results streamed incrementally to *output_path*.
    """
    sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)

    rhs_pf = pq.ParquetFile(str(rhs_parquet))
    n_rgs = rhs_pf.metadata.num_row_groups
    rgs_per_chunk = max(1, (n_rgs + n_chunks - 1) // n_chunks)
    slices = [
        list(range(s, min(s + rgs_per_chunk, n_rgs)))
        for s in range(0, n_rgs, rgs_per_chunk)
    ]
    log.debug("RHS slices are: %s", slices)

    bbox_idx = _bbox_col_indices(rhs_pf)
    log.debug("bbox_idx looks like: %s", bbox_idx)

    log.info(
        "RHS (%s): %d row groups → %d chunks; streaming to %s",
        rhs_view,
        n_rgs,
        len(slices),
        output_path,
    )

    writer: pq.ParquetWriter | None = None
    try:
        for i, phase2_rhs_slice in enumerate(slices):
            chunk = rhs_pf.read_row_groups(phase2_rhs_slice)
            envelope = _slice_envelope(rhs_pf, phase2_rhs_slice, bbox_idx)

            _register_rhs_view(sd, chunk, rhs_view)

            query = query_template.format(
                spatial_filter=_usrn_spatial_filter(envelope, usrn_expand_m)
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
                    "Chunk %d/%d (%d rhs rgs): %d matches",
                    i + 1,
                    len(slices),
                    len(phase2_rhs_slice),
                    len(result),
                )
    finally:
        if writer:
            writer.close()


def _national_three_phase(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    intersect_template: str,
    phase2_template: str,
    phase3_template: str,
    phase4_template: str,
    id_col: str,
    max_d: float,
    explain: bool,
    output_path: pathlib.Path,
    usrn_line_parquet: pathlib.Path,
    n_chunks: int = 50,
    overlap_threshold: float = 0.10,
    phase4_tolerance_m: float = 5.0,
) -> None:
    """Three-phase NationalMode executor — two USRN files, chunked RHS.

    Used exclusively by ``line`` joins.
    Phase 1: ST_Intersects against ``usrns`` (centrelines); all touching pairs kept.
    Phase 2: ST_Intersects against ``usrns_line`` (buffered corridors) for unmatched rows only,
             sub-chunked into 5 000-row batches with per-sub-chunk USRN pruning.
    Phase 3: ST_DWithin nearest ``usrns`` for features still unmatched after Phase 2,
             same sub-chunking with envelope expanded by max_d.
    Results streamed incrementally to *output_path*.
    """
    sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)
    sd.read_parquet(str(usrn_line_parquet)).to_view("usrns_line", overwrite=True)

    rhs_pf = pq.ParquetFile(str(rhs_parquet))
    n_rgs = rhs_pf.metadata.num_row_groups
    rgs_per_chunk = max(1, (n_rgs + n_chunks - 1) // n_chunks)
    slices = [
        list(range(s, min(s + rgs_per_chunk, n_rgs)))
        for s in range(0, n_rgs, rgs_per_chunk)
    ]

    bbox_idx = _bbox_col_indices(rhs_pf)

    log.info(
        "RHS (%s): %d row groups → %d chunks (three-phase intersect-first); streaming to %s",
        rhs_view,
        n_rgs,
        len(slices),
        output_path,
    )

    # Match-rate accumulators. Each RHS feature lives in exactly one row-group slice,
    # so summing per-chunk distinct counts gives the global figures.
    total_features = 0
    sum_p1 = sum_p2 = sum_p3 = sum_p4 = 0
    writer: pq.ParquetWriter | None = None
    try:
        for i, rg_slice in enumerate(slices):
            log.info(
                "── Chunk %d/%d (%d rhs row groups) ──",
                i + 1,
                len(slices),
                len(rg_slice),
            )

            # Accumulated results — one list per phase (reset each chunk)
            phase1_parts: list[pa.Table] = []
            phase2_parts: list[pa.Table] = []
            phase3_parts: list[pa.Table] = []

            chunk = rhs_pf.read_row_groups(rg_slice)
            envelope = _slice_envelope(rhs_pf, rg_slice, bbox_idx)

            _register_rhs_view(sd, chunk, rhs_view)

            # Phase 1: exact chunk envelope (no expansion) — only USRNs that touch the chunk
            phase1_filter = _usrn_spatial_filter(envelope)

            log.debug("  Phase 1 (intersect) on %d rows...", len(chunk))
            # Corridor side expanded by max_d: a buffer reaches that far beyond its
            # centreline, so an unexpanded envelope would prune corridors the join needs.
            intersect_query = intersect_template.format(
                spatial_filter=phase1_filter,
                corridor_filter=_usrn_spatial_filter(envelope, max_d, alias="ul"),
            )
            if explain and i == 0:
                log_plan(sd, intersect_query)
            intersect_result = _compute_overlap(
                _normalise_arrow(
                    cast(pa.Table, sd.sql(intersect_query).to_arrow_table())
                ),
                max_d,
            )
            log.info("  Phase 1 (intersect): %d matches", len(intersect_result))

            if len(intersect_result):
                phase1_parts.append(intersect_result)
                matched_ids: set = set(intersect_result.column(id_col).to_pylist())
                unmatched_chunk = chunk.filter(
                    _anti_join_mask(chunk, id_col, matched_ids)
                )
            else:
                unmatched_chunk = chunk

            # Phase 2: sub-chunked, per-sub-chunk envelope — buffered geometry covers corridor
            phase2_matched_ids: set = set()
            if len(unmatched_chunk):
                phase2_parts = _run_sub_batches(
                    sd,
                    unmatched_chunk,
                    rhs_view,
                    phase2_template,
                    expand_m=0.0,
                    post_process=lambda t: _overlap_post_filter(
                        t, id_col, max_d, overlap_threshold
                    ),
                    explain=explain and i == 0,
                    label="Phase 2 (corridor)",
                    indent="  ",
                )

            if phase2_parts:
                phase2_table = pa.concat_tables(phase2_parts)
                phase2_matched_ids = set(phase2_table.column(id_col).to_pylist())
                log.info(
                    "  Phase 2 (corridor): %d matches total",
                    len(phase2_table),
                )

            # Phase 3: ST_DWithin nearest USRN for features unmatched by Phase 1 and 2
            phase3_rhs_slice = (
                unmatched_chunk.filter(
                    _anti_join_mask(unmatched_chunk, id_col, phase2_matched_ids)
                )
                if len(unmatched_chunk)
                else unmatched_chunk
            )

            if len(phase3_rhs_slice):
                # expand_m=max_d so USRNs just outside the slice bounds stay reachable
                phase3_parts = _run_sub_batches(
                    sd,
                    phase3_rhs_slice,
                    rhs_view,
                    phase3_template,
                    expand_m=max_d,
                    post_process=lambda t: _phase3_nearest_dedup(t, id_col),
                    explain=explain and i == 0,
                    label="Phase 3 (nearest)",
                    indent="  ",
                )

            if phase3_parts:
                phase3_table = pa.concat_tables(phase3_parts)
                log.info("  Phase 3 (nearest): %d matches", len(phase3_table))

            # Phase 4: inherit a USRN across a physical connection. Confined to this
            # chunk — Hilbert ordering keeps connected features together, so the loss
            # against a global pass is under half a percentage point.
            phase4_parts = _propagate_phase4(
                sd,
                chunk,
                phase1_parts + phase2_parts + phase3_parts,
                rhs_view,
                phase4_template,
                id_col=id_col,
                tolerance_m=phase4_tolerance_m,
                expand_m=max_d,
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
            total_features += len(chunk)
            sum_p1 += _distinct_count(phase1_parts, id_col)
            sum_p2 += _distinct_count(phase2_parts, id_col)
            sum_p3 += _distinct_count(phase3_parts, id_col)
            sum_p4 += _distinct_count(phase4_parts, id_col)

            log.info(
                "  Chunk %d/%d done: %d intersect + %d corridor + %d nearest + %d connected rows",
                i + 1,
                len(slices),
                sum(len(t) for t in phase1_parts),
                sum(len(t) for t in phase2_parts),
                sum(len(t) for t in phase3_parts),
                sum(len(t) for t in phase4_parts),
            )
    finally:
        if writer:
            writer.close()

    _log_line_match_summary(total_features, sum_p1, sum_p2, sum_p3, sum_p4)


def _filtered_three_phase(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    intersect_template: str,
    phase2_template: str,
    phase3_template: str,
    phase4_template: str,
    id_col: str,
    max_d: float,
    bbox: BBox,
    explain: bool,
    overlap_threshold: float,
    usrn_line_parquet: pathlib.Path,
    phase4_tolerance_m: float = 5.0,
) -> pa.Table:
    """Three-phase FilteredMode executor for ``line`` joins — two USRN files, city bbox.

    Phase 1 — ``ST_Intersects`` against ``usrns`` with exact bbox prune; all
    touching pairs kept; ``_compute_overlap`` adds ``overlap_length_pct`` and
    ``match_phase=1``.

    Phase 2 — ``ST_Intersects`` against ``usrns_line`` with (buffered corridors) for
    unmatched RHS features only. The unmatched set is computed in Python (set
    lookup). Unmatched rows are chunked (5 000 rows) so each sub-query has a tight USRN
    spatial filter. ``_overlap_post_filter`` keeps the best-corridor USRN(s) per
    RHS feature and appends ``match_phase=2``.

    Phase 3 — ``ST_DWithin`` against ``usrns`` for features still unmatched after
    Phase 2. ``_phase3_nearest_dedup`` picks the single closest USRN per feature
    and appends ``match_phase=3``.
    """

    # Accumulated results — one list per phase
    phase1_parts: list[pa.Table] = []
    phase2_parts: list[pa.Table] = []
    phase3_parts: list[pa.Table] = []

    # Read files in as views
    sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)
    sd.read_parquet(str(usrn_line_parquet)).to_view("usrns_line", overwrite=True)
    sd.read_parquet(str(rhs_parquet)).to_view(rhs_view, overwrite=True)

    # Phase 1: exact ST_Intersects with standard bbox prune (no expansion)
    phase1_filter = _bbox_pruner(bbox)
    log.debug("Phase 1 template (before filter):\n%s", intersect_template)

    intersect_query = intersect_template.format(
        spatial_filter=phase1_filter,
        corridor_filter=_usrn_spatial_filter(bbox, max_d, alias="ul"),
    )
    log.debug("Phase 1 query (after filter):\n%s", intersect_query)

    if explain:
        log_plan(sd, intersect_query)
    intersect_result = _compute_overlap(
        _normalise_arrow(cast(pa.Table, sd.sql(intersect_query).to_arrow_table())),
        max_d,
    )
    log.info("Phase 1 (intersect): %d matches", len(intersect_result))

    # If there's stuff to collect put it in phase1 list
    if len(intersect_result):
        phase1_parts.append(intersect_result)

    # Phase 2: load expanded-bbox RHS slice into Python, drop Phase 1 matched IDs from data,
    # Reload in the same data as phase 1 but slightly expanded bbox
    xmin, ymin, xmax, ymax = bbox
    ex = max_d
    phase2_rhs_slice: pa.Table = pq.read_table(
        str(rhs_parquet),
        filters=(
            (pc.field("bbox", "xmax") >= xmin - ex)
            & (pc.field("bbox", "xmin") <= xmax + ex)
            & (pc.field("bbox", "ymax") >= ymin - ex)
            & (pc.field("bbox", "ymin") <= ymax + ex)
        ),
    )
    log.info(
        "Phase 2 (corridor): %d candidate rows loaded (expanded bbox)",
        len(phase2_rhs_slice),
    )
    # Total RHS features in scope (expanded-bbox universe) — the "started with" figure
    # for the match-rate summary. Captured before Phase 1 matches are filtered out.
    total_features = len(phase2_rhs_slice)
    # Every feature in scope, with geometry — Phase 4 needs the matched ones to
    # propagate from, so keep a handle before the slice is narrowed phase by phase.
    all_features: pa.Table = phase2_rhs_slice

    if phase1_parts:
        # Remove phase 1 matches from phase 2 slice
        matched_ids: set = set(intersect_result.column(id_col).to_pylist())
        log.debug("Matched IDs from Phase 1: %s", matched_ids)
        phase2_rhs_slice = phase2_rhs_slice.filter(
            _anti_join_mask(phase2_rhs_slice, id_col, matched_ids)
        )
        log.info(
            "Phase 2 (corridor): %d unmatched rows after dropping Phase 1 matches",
            len(phase2_rhs_slice),
        )

    if len(phase2_rhs_slice):
        # Sub-batched so each sub-query sees only the USRN row groups overlapping that
        # small area — avoids rescanning the whole bbox. expand_m=0.0: the buffered
        # corridor geometry already covers the reach.
        phase2_parts = _run_sub_batches(
            sd,
            phase2_rhs_slice,
            rhs_view,
            phase2_template,
            expand_m=0.0,
            post_process=lambda t: _overlap_post_filter(
                t, id_col, max_d, overlap_threshold
            ),
            explain=explain,
            label="Phase 2 (corridor)",
        )

    if phase2_parts:
        phase2_table = pa.concat_tables(phase2_parts)
        log.info("Phase 2 (corridor): %d matches total", len(phase2_table))
        # Remove phase 2 matches from phase 3 slice
        phase2_matched_ids: set = set(phase2_table.column(id_col).to_pylist())
        phase3_rhs_slice = phase2_rhs_slice.filter(
            _anti_join_mask(phase2_rhs_slice, id_col, phase2_matched_ids)
        )
    else:
        # If phase 2 matched nothing then reuse the entire phase 2 slice
        # for phase 3
        phase3_rhs_slice = phase2_rhs_slice

    # Phase 3: ST_DWithin nearest USRN for features still unmatched after Phase 2
    if len(phase3_rhs_slice):
        # expand_m=max_d matches _national_three_phase — without it a USRN within
        # phase3_distance of a feature but outside the sub-batch bounds is missed.
        phase3_parts = _run_sub_batches(
            sd,
            phase3_rhs_slice,
            rhs_view,
            phase3_template,
            expand_m=max_d,
            post_process=lambda t: _phase3_nearest_dedup(t, id_col),
            explain=explain,
            label="Phase 3 (nearest)",
        )

    if phase3_parts:
        phase3_table = pa.concat_tables(phase3_parts)
        log.info("Phase 3 (nearest): %d matches", len(phase3_table))

    # Phase 4: inherit a USRN across a physical connection to an already-matched feature
    phase4_parts = _propagate_phase4(
        sd,
        all_features,
        phase1_parts + phase2_parts + phase3_parts,
        rhs_view,
        phase4_template,
        id_col=id_col,
        tolerance_m=phase4_tolerance_m,
        expand_m=max_d,
        explain=explain,
    )
    if phase4_parts:
        log.info(
            "Phase 4 (connected): %d matches",
            sum(len(t) for t in phase4_parts),
        )

    _log_line_match_summary(
        total_features,
        _distinct_count(phase1_parts, id_col),
        _distinct_count(phase2_parts, id_col),
        _distinct_count(phase3_parts, id_col),
        _distinct_count(phase4_parts, id_col),
    )

    all_parts = phase1_parts + phase2_parts + phase3_parts + phase4_parts
    return pa.concat_tables(all_parts) if all_parts else pa.table({})


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _duck() -> duckdb.DuckDBPyConnection:
    """Shared DuckDB connection for the overlap/dedup post-processors.

    The spatial extension is installed and loaded exactly once per process here.
    The post-processors re-``register("t", table)`` on each call (which rebinds the
    view), so reusing this connection avoids paying ``LOAD spatial`` per sub-chunk —
    in a national three-phase run that is dozens-to-hundreds of avoided loads.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


# Overlap scoring for Phases 1 and 2. ``_u_geom`` is the pre-buffered corridor polygon
# from ``usrns_line.geometry`` in both cases, so it is intersected directly.
#
# Phase 1 matches against raw centrelines, so it joins ``usrns_line`` on ``usrn`` purely
# to fetch that corridor. Buffering the centreline per row here instead — which is what
# this used to do — cost 97-99% of Phase 1's wall time (78s of an 82s chunk on
# gas_pipe), because ST_Buffer scales with vertex count. The corridor is already on
# disk; reading it is ~20-57x faster and produces bit-identical scores.
#
# This does couple the score to the corridor file's buffer width rather than to
# ``--distance``: if ``prepare-usrns-line --buffer-m`` differs from the match-time
# ``--distance``, Phase 1's overlap is measured against the file's width. Phase 2 has
# always behaved that way, so the two phases are now consistent.
_OVERLAP_EXPR_CORRIDOR = """
    COALESCE(
        ST_Length(
            ST_Intersection(
                ST_GeomFromWKB(_u_geom),
                ST_GeomFromWKB(_s_geom)
            )
        ) / NULLIF(GREATEST(ST_Length(ST_GeomFromWKB(_s_geom)), {denom}), 0),
        0.0
    )
"""


def _compute_overlap(table: pa.Table, max_d: float) -> pa.Table:
    """Add ``overlap_length_pct`` and ``match_phase=1`` to Phase 1 results — all pairs kept."""
    con = _duck()
    con.register("t", table)
    expr = _OVERLAP_EXPR_CORRIDOR.format(denom=2 * max_d)
    result = con.execute(f"""
        SELECT * EXCLUDE (_u_geom, _s_geom),
            {expr} AS overlap_length_pct
        FROM t
    """).fetch_arrow_table()
    return result.append_column(
        "match_phase", pa.array([1] * len(result), type=pa.int8())
    )


def _overlap_post_filter(
    table: pa.Table, id_col: str, max_d: float, min_overlap: float = 0.10
) -> pa.Table:
    """Keep the best USRN corridor match(es) per Phase 2 RHS feature.

    Drops features whose best candidate is below ``min_overlap`` (default 10 %) —
    a sub-threshold match is a crossing, not a corridor relationship. Among passing
    features, keeps all USRNs within 80 % of the best overlap so a feature straddling
    two streets gets both.

    ``_u_geom`` is the pre-buffered corridor polygon (usrns_line.geometry), so the
    overlap is scored against the same corridor the Phase 2 join matched against.
    """
    con = _duck()
    con.register("t", table)
    expr = _OVERLAP_EXPR_CORRIDOR.format(max_d=max_d, denom=2 * max_d)
    result: pa.Table = con.execute(f"""
        WITH scored AS (
            SELECT * EXCLUDE (_u_geom, _s_geom),
                {expr} AS overlap_length_pct
            FROM t
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
    """).fetch_arrow_table()
    log.debug(
        "Phase 2 (corridor) overlap: %d → %d rows kept (best per feature)",
        len(table),
        len(result),
    )
    return result.append_column(
        "match_phase", pa.array([2] * len(result), type=pa.int8())
    )


def _phase3_nearest_dedup(table: pa.Table, id_col: str, phase: int = 3) -> pa.Table:
    """Pick the single closest USRN per unmatched RHS feature.

    Used by the Phase 3 nearest fallback and by Phase 4, where several connected
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
    """).fetch_arrow_table()
    return result.append_column(
        "match_phase", pa.array([phase] * len(result), type=pa.int8())
    )


def _register_neighbours_view(
    sd: SedonaContext, features: pa.Table, matched: pa.Table, id_col: str
) -> int:
    """Register already-matched RHS features as the ``neighbours`` view for Phase 4.

    Each matched feature contributes its geometry plus the single best USRN it
    resolved to — lowest ``match_phase`` first (an intersection beats a corridor beats
    a nearest hit), then shortest ``distance_m``. Phase 4 propagates that USRN across
    physical connections, so seeding it with the feature's strongest match keeps the
    inherited value as trustworthy as the evidence allows.

    Returns the number of neighbour rows registered (0 if there is nothing to seed from).
    """
    if not len(matched) or not len(features):
        return 0
    con = _duck()
    con.register("_feat", features)
    con.register("_matched", matched)
    neighbours = con.execute(f"""
        WITH best AS (
            SELECT "{id_col}" AS _id, usrn, street_type,
                ROW_NUMBER() OVER (
                    PARTITION BY "{id_col}" ORDER BY match_phase, distance_m, usrn
                ) AS _rn
            FROM _matched
        )
        SELECT f.geometry AS geometry, b.usrn AS usrn, b.street_type AS street_type
        FROM best AS b
        JOIN _feat AS f ON f."{id_col}" = b._id
        WHERE b._rn = 1
    """).fetch_arrow_table()
    if not len(neighbours):
        return 0
    sd.create_data_frame(neighbours).to_view("neighbours_raw", overwrite=True)
    sd.sql(
        "SELECT usrn, street_type,"
        " ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM neighbours_raw"
    ).to_view("neighbours", overwrite=True)
    return len(neighbours)


# ---------------------------------------------------------------------------
# Join strategies
#
# Three strategies, two architectures:
#
#   polygon  — polygon datasets (e.g. soil, land cover)
#              Single-phase · 1 USRN file (usrns_27700.parquet)
#              ST_Intersects(u.geometry, s.geometry)
#
#   point    — point datasets (e.g. bus stops, traffic counts)
#              Single-phase · 1 USRN file (usrns_27700.parquet)
#              ST_DWithin(u.geometry, s.geometry, distance_m)
#
#   line     — linestring datasets (e.g. gas pipes, cables)
#              Three-phase · 2 USRN files (usrns_27700.parquet + usrns_line_Nm_27700.parquet)
#              Phase 1: ST_Intersects against centrelines (all touching pairs kept)
#              Phase 2: ST_Intersects against pre-buffered corridor polygons (overlap-filtered)
#              Phase 3: ST_DWithin nearest USRN for features still unmatched after Phase 2
# ---------------------------------------------------------------------------


@register(GeometryType.POLYGON)
def run_polygon_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: AnalysisMode = _DEFAULT_MODE,
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

    col_fragment: str = _col_fragment(rhs_config)

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {col_fragment}
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
        filter_fn=_bbox_pruner,
        explain=explain,
        output_path=output_path,
        usrn_expand_m=0.0,
    )


@register(GeometryType.POINT)
def run_point_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: AnalysisMode = _DEFAULT_MODE,
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

    col_fragment: str = _col_fragment(rhs_config)

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {col_fragment},
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
        filter_fn=lambda tile: _bbox_nearest_filters(tile, distance_m),
        explain=explain,
        output_path=output_path,
        usrn_expand_m=distance_m,
    )


@register(GeometryType.LINE)
def run_line_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: AnalysisMode = _DEFAULT_MODE,
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
    """Match each linestring in the RHS dataset to USRNs — always three-phase, two USRN files.

    Phase 1 uses ``usrns_27700.parquet`` (centrelines) with ``ST_Intersects`` —
    touching pairs are definitive matches (``match_phase=1``).

    Phase 2 uses ``usrns_line_Nm_27700.parquet`` (pre-buffered corridor polygons)
    with ``ST_Intersects`` for unmatched RHS lines only. ``_overlap_post_filter``
    selects the USRN(s) whose corridor best covers each line (``match_phase=2``).

    Phase 3 uses ``usrns_27700.parquet`` with ``ST_DWithin`` for features still
    unmatched after Phase 2. The single closest USRN per feature is kept
    (``match_phase=3``). ``overlap_length_pct`` is ``0.0`` for Phase 3 rows.

    ``rhs_id_col`` is required to track which RHS features are matched between phases.
    """
    if not rhs_id_col:
        raise ValueError(
            "run_line_join requires --rhs-id-col to track matched RHS features between phases."
        )
    if usrn_line_parquet is None:
        raise ValueError(
            "run_line_join requires --usrn-line-parquet. "
            "Run 'usrn-matcher prepare-usrns-line --buffer-m N' first."
        )
    rhs_view: str = rhs_config.name

    match mode:
        case FilteredMode(bbox=bbox):
            log.info(
                "Bbox filter three-phase (USRNs exact, RHS expanded by %.0fm): xmin=%s ymin=%s xmax=%s ymax=%s",
                distance_m,
                *bbox,
            )
        case NationalMode():
            log.info("No bbox supplied — matching all lines (three-phase).")

    col_fragment: str = _col_fragment(rhs_config)

    # The match itself is against the raw centreline (u.geometry). ``usrns_line`` is
    # joined on usrn only to fetch that USRN's pre-buffered corridor for the overlap
    # score — see _OVERLAP_EXPR_CORRIDOR for why this beats buffering per row.
    intersect_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {col_fragment},
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
    buffered_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {col_fragment},
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
    _p3_dist: float = phase3_distance_m if phase3_distance_m is not None else distance_m
    phase3_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {col_fragment},
                ST_Distance(u.geometry, s.geometry) AS distance_m,
                FALSE AS is_intersection,
                CAST(0.0 AS DOUBLE) AS overlap_length_pct
            FROM usrns AS u
            JOIN {rhs_view} AS s ON ST_DWithin(u.geometry, s.geometry, {_p3_dist})
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
                {col_fragment},
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
        intersect_template,
        buffered_template,
        phase3_template,
        phase4_template,
        id_col=rhs_id_col,
        max_d=distance_m,
        mode=mode,
        usrn_line_parquet=usrn_line_parquet,
        explain=explain,
        output_path=output_path,
        overlap_threshold=overlap_threshold,
        phase4_tolerance_m=phase4_tolerance_m,
    )


# ---------------------------------------------------------------------------
# SQL fragment helpers / Spatial predicates
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=None)
def _read_auto_cols(parquet_path: str) -> tuple[str, ...]:
    """Return non-geometry column names from a parquet footer — cached per path."""
    schema = pq.read_schema(parquet_path)
    return tuple(name for name in schema.names if name not in ("geometry", "bbox"))


def _col_fragment(rhs_config: DatasetConfig) -> str:
    """Return the SELECT fragment for RHS columns (prefixed with a leading comma).

    If ``rhs_config.columns`` is non-empty the listed columns are used as-is.
    Otherwise all columns except ``geometry`` and the internal ``bbox`` covering
    column are discovered from the parquet file footer.
    """
    if rhs_config.columns:
        return ", " + ", ".join(f's."{c}"' for c in rhs_config.columns)
    # rhs_df.schema is a PySedonaSchema (not iterable like PyArrow) so read
    # column names directly from the parquet file footer instead.
    # Result is cached per path — only one I/O hit per process.
    return ", " + ", ".join(
        f's."{c}"' for c in _read_auto_cols(str(rhs_config.parquet_path))
    )


def _bbox_pruner(bbox: BBox) -> str:
    """Return AND conditions that prune both parquet scans to the given EPSG:27700 bbox.

    Uses ST_Intersects against a fixed bbox polygon so Sedona can skip row groups
    via GeoParquet 1.1 covering metadata (row_groups_spatial_pruned path).
    Called by ``execute_join`` as the ``filter_fn`` for intersect-style joins.
    """
    xmin, ymin, xmax, ymax = bbox
    polygon_wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    bbox_geom = f"ST_SetSRID(ST_GeomFromWKT('{polygon_wkt}'), 27700)"
    return (
        f"AND ST_Intersects(u.geometry, {bbox_geom})"
        f" AND ST_Intersects(s.geometry, {bbox_geom})"
    )


def _bbox_nearest_filters(bbox: BBox, distance_m: float) -> str:
    """Return AND conditions that prune both sides of a nearest/line join.

    USRNs (``u``) are filtered to the exact bbox; RHS (``s``) is expanded by
    ``distance_m`` so features just outside the boundary can still match.

    Uses ST_Intersects against fixed bbox polygons so Sedona can skip row groups
    via GeoParquet 1.1 covering metadata (row_groups_spatial_pruned path).
    """
    xmin, ymin, xmax, ymax = bbox

    def _pred(alias: str, x0: float, y0: float, x1: float, y1: float) -> str:
        wkt = f"POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))"
        return f"ST_Intersects({alias}.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"

    return (
        f"AND {_pred('u', xmin, ymin, xmax, ymax)}"
        f" AND {_pred('s', xmin - distance_m, ymin - distance_m, xmax + distance_m, ymax + distance_m)}"
    )
