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

from .config import BBox, DatasetConfig
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
        xmin, ymin, xmax, ymax = self.bbox
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
    ) -> pa.Table: ...


_registry: dict[str, JoinFn] = {}
_J = TypeVar("_J", bound=JoinFn)


def register(name: str) -> Callable[[_J], _J]:
    """Register a join function under *name*"""

    def decorator(fn: _J) -> _J:
        _registry[name] = fn
        return fn

    return decorator


def get_join(name: str) -> JoinFn:
    if name not in _registry:
        raise KeyError(f"No join registered for '{name}'. Available: {list(_registry)}")
    return _registry[name]


# ---------------------------------------------------------------------------
# Session configuration
# ---------------------------------------------------------------------------


def configure_session(sd: SedonaContext, target_partitions: int = 4) -> None:
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


# ---------------------------------------------------------------------------
# Core executor
# ---------------------------------------------------------------------------


# TODO: bring into a single dispatch method again
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

    FilteredMode: both parquets registered as Sedona views; query run directly — logic
    inlined here, no helper function.

    NationalMode: delegates to ``_national_single_phase`` which chunks the RHS and streams
    results. ``usrn_expand_m`` expands the USRN spatial filter per chunk (= ``distance_m``
    for point joins so USRNs just outside the raw chunk envelope remain reachable).
    """
    rhs_meta = pq.read_metadata(str(rhs_parquet))

    match mode:
        case FilteredMode(bbox=bbox):
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
    id_col: str,
    max_d: float,
    mode: AnalysisMode,
    usrn_line_parquet: pathlib.Path,
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    overlap_threshold: float = 0.10,
) -> pa.Table:
    """Three-phase dispatcher for ``line`` joins — always uses two USRN parquets.

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
                id_col,
                max_d,
                bbox,
                explain,
                overlap_threshold,
                usrn_line_parquet,
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
                    id_col,
                    max_d,
                    explain,
                    output_path,
                    usrn_line_parquet,
                    n_chunks=n_chunks,
                    overlap_threshold=overlap_threshold,
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
                    id_col,
                    max_d,
                    explain=explain,
                    output_path=_path,
                    n_chunks=n_chunks,
                    overlap_threshold=overlap_threshold,
                    usrn_line_parquet=usrn_line_parquet,
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
    log.debug(f"RHS slices are: {slices}")

    bbox_idx = _bbox_col_indices(rhs_pf)
    log.debug(f"bbox_idx looks like:{bbox_idx}")

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

            non_geom = ", ".join(
                f'"{c}"' for c in chunk.schema.names if c != "geometry"
            )
            sd.create_data_frame(chunk).to_view("rhs_raw", overwrite=True)
            sd.sql(
                f"SELECT {non_geom},"
                f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
            ).to_view(rhs_view, overwrite=True)

            xmin, ymin, xmax, ymax = envelope
            ex = usrn_expand_m
            wkt = (
                f"POLYGON(({xmin - ex} {ymin - ex},{xmax + ex} {ymin - ex},"
                f"{xmax + ex} {ymax + ex},{xmin - ex} {ymax + ex},{xmin - ex} {ymin - ex}))"
            )
            spatial_filter = f"AND ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"
            query = query_template.format(spatial_filter=spatial_filter)
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
    id_col: str,
    max_d: float,
    explain: bool,
    output_path: pathlib.Path,
    usrn_line_parquet: pathlib.Path,
    n_chunks: int = 50,
    overlap_threshold: float = 0.10,
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

    _ROWS_PER_CHUNK = 5_000
    writer: pq.ParquetWriter | None = None
    try:
        for i, rg_slice in enumerate(slices):
            log.debug(
                "── Chunk %d/%d (row groups %s) ──",
                i + 1,
                len(slices),
                rg_slice,
            )

            # Accumulated results — one list per phase (reset each chunk)
            phase1_parts: list[pa.Table] = []
            phase2_parts: list[pa.Table] = []
            phase3_parts: list[pa.Table] = []

            chunk = rhs_pf.read_row_groups(rg_slice)
            envelope = _slice_envelope(rhs_pf, rg_slice, bbox_idx)
            xmin, ymin, xmax, ymax = envelope

            non_geom = ", ".join(
                f'"{c}"' for c in chunk.schema.names if c != "geometry"
            )
            sd.create_data_frame(chunk).to_view("rhs_raw", overwrite=True)
            sd.sql(
                f"SELECT {non_geom},"
                f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
            ).to_view(rhs_view, overwrite=True)

            # Phase 1: exact chunk envelope (no expansion) — only USRNs that touch the chunk
            wkt1 = (
                f"POLYGON(({xmin} {ymin},{xmax} {ymin},"
                f"{xmax} {ymax},{xmin} {ymax},{xmin} {ymin}))"
            )
            phase1_filter = f"AND ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt1}'), 27700))"

            log.debug("  Phase 1 — ST_Intersects join (%d RHS rows)...", len(chunk))
            intersect_query = intersect_template.format(spatial_filter=phase1_filter)
            if explain and i == 0:
                log_plan(sd, intersect_query)
            intersect_result = _compute_overlap(
                _normalise_arrow(
                    cast(pa.Table, sd.sql(intersect_query).to_arrow_table())
                ),
                max_d,
            )
            log.debug("  Phase 1: %d USRN-RHS intersect pairs", len(intersect_result))

            if len(intersect_result):
                phase1_parts.append(intersect_result)
                matched_ids: set = set(intersect_result.column(id_col).to_pylist())
                unmatched_chunk = chunk.filter(
                    pa.array(
                        [v not in matched_ids for v in chunk.column(id_col).to_pylist()]
                    )
                )
            else:
                unmatched_chunk = chunk

            # Phase 2: sub-chunked, per-sub-chunk envelope — buffered geometry covers corridor
            phase2_matched_ids: set = set()
            if len(unmatched_chunk):
                non_geom_u = ", ".join(
                    f'"{c}"' for c in unmatched_chunk.schema.names if c != "geometry"
                )
                n_chunks = max(
                    1, (len(unmatched_chunk) + _ROWS_PER_CHUNK - 1) // _ROWS_PER_CHUNK
                )
                log.debug(
                    "  Phase 2 — ST_Intersects (buffered) on %d unmatched RHS rows → %d sub-chunks...",
                    len(unmatched_chunk),
                    n_chunks,
                )

                for sub_i in range(n_chunks):
                    sub = unmatched_chunk.slice(
                        sub_i * _ROWS_PER_CHUNK, _ROWS_PER_CHUNK
                    )
                    if not len(sub):
                        continue

                    bbox_col = sub.column("bbox")
                    xmin_c = pc.min(pc.struct_field(bbox_col, "xmin")).as_py()
                    ymin_c = pc.min(pc.struct_field(bbox_col, "ymin")).as_py()
                    xmax_c = pc.max(pc.struct_field(bbox_col, "xmax")).as_py()
                    ymax_c = pc.max(pc.struct_field(bbox_col, "ymax")).as_py()

                    sd.create_data_frame(sub).to_view("rhs_raw", overwrite=True)
                    sd.sql(
                        f"SELECT {non_geom_u},"
                        f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
                    ).to_view(rhs_view, overwrite=True)

                    wkt_sub = (
                        f"POLYGON(({xmin_c} {ymin_c},{xmax_c} {ymin_c},"
                        f"{xmax_c} {ymax_c},{xmin_c} {ymax_c},{xmin_c} {ymin_c}))"
                    )
                    usrn_filter = f"AND ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt_sub}'), 27700))"
                    phase2_query = phase2_template.format(spatial_filter=usrn_filter)
                    if explain and i == 0 and sub_i == 0:
                        log_plan(sd, phase2_query)

                    phase2_result = _normalise_arrow(
                        cast(pa.Table, sd.sql(phase2_query).to_arrow_table())
                    )
                    log.debug(
                        "  Phase 2 sub-chunk %d/%d (%d rows): %d candidates",
                        sub_i + 1,
                        n_chunks,
                        len(sub),
                        len(phase2_result),
                    )
                    if len(phase2_result):
                        filtered = _overlap_post_filter(
                            phase2_result, id_col, max_d, overlap_threshold
                        )
                        if len(filtered):
                            phase2_parts.append(filtered)

            if phase2_parts:
                phase2_table = pa.concat_tables(phase2_parts)
                phase2_matched_ids = set(phase2_table.column(id_col).to_pylist())
                log.debug(
                    "  Phase 2 after overlap filter: %d matches total",
                    len(phase2_table),
                )

            # Phase 3: ST_DWithin nearest USRN for features unmatched by Phase 1 and 2
            phase3_rhs_slice = (
                unmatched_chunk.filter(
                    pa.array(
                        [
                            v not in phase2_matched_ids
                            for v in unmatched_chunk.column(id_col).to_pylist()
                        ]
                    )
                )
                if len(unmatched_chunk)
                else unmatched_chunk
            )

            if len(phase3_rhs_slice):
                non_geom_p3 = ", ".join(
                    f'"{c}"' for c in phase3_rhs_slice.schema.names if c != "geometry"
                )
                n_chunks_p3 = max(
                    1, (len(phase3_rhs_slice) + _ROWS_PER_CHUNK - 1) // _ROWS_PER_CHUNK
                )
                log.debug(
                    "  Phase 3 — ST_DWithin nearest on %d still-unmatched RHS rows → %d sub-chunks...",
                    len(phase3_rhs_slice),
                    n_chunks_p3,
                )

                for j in range(n_chunks_p3):
                    sub = phase3_rhs_slice.slice(j * _ROWS_PER_CHUNK, _ROWS_PER_CHUNK)
                    if not len(sub):
                        continue

                    bbox_col = sub.column("bbox")
                    xmin_c = pc.min(pc.struct_field(bbox_col, "xmin")).as_py()
                    ymin_c = pc.min(pc.struct_field(bbox_col, "ymin")).as_py()
                    xmax_c = pc.max(pc.struct_field(bbox_col, "xmax")).as_py()
                    ymax_c = pc.max(pc.struct_field(bbox_col, "ymax")).as_py()

                    sd.create_data_frame(sub).to_view("rhs_raw", overwrite=True)
                    sd.sql(
                        f"SELECT {non_geom_p3},"
                        f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
                    ).to_view(rhs_view, overwrite=True)

                    # Expand sub-chunk envelope by max_d so USRNs just outside can still match
                    wkt3 = (
                        f"POLYGON(({xmin_c - max_d} {ymin_c - max_d},{xmax_c + max_d} {ymin_c - max_d},"
                        f"{xmax_c + max_d} {ymax_c + max_d},{xmin_c - max_d} {ymax_c + max_d},"
                        f"{xmin_c - max_d} {ymin_c - max_d}))"
                    )
                    usrn_filter = f"AND ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt3}'), 27700))"
                    phase3_query = phase3_template.format(spatial_filter=usrn_filter)
                    if explain and i == 0 and j == 0:
                        log_plan(sd, phase3_query)
                    phase3_raw = _normalise_arrow(
                        cast(pa.Table, sd.sql(phase3_query).to_arrow_table())
                    )
                    if len(phase3_raw):
                        deduped = _phase3_nearest_dedup(phase3_raw, id_col)
                        if len(deduped):
                            phase3_parts.append(deduped)

            if phase3_parts:
                phase3_table = pa.concat_tables(phase3_parts)
                log.debug("  Phase 3 nearest: %d matches", len(phase3_table))

            # Write all phases for this chunk
            all_parts = phase1_parts + phase2_parts + phase3_parts
            if all_parts:
                chunk_result = pa.concat_tables(all_parts)
                if writer is None:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(str(output_path), chunk_result.schema)
                writer.write_table(chunk_result)

            log.info(
                "Chunk %d/%d (%d rhs rgs): %d intersect + %d proximity + %d nearest",
                i + 1,
                len(slices),
                len(rg_slice),
                sum(len(t) for t in phase1_parts),
                sum(len(t) for t in phase2_parts),
                sum(len(t) for t in phase3_parts),
            )
    finally:
        if writer:
            writer.close()


def _filtered_three_phase(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    intersect_template: str,
    phase2_template: str,
    phase3_template: str,
    id_col: str,
    max_d: float,
    bbox: BBox,
    explain: bool,
    overlap_threshold: float,
    usrn_line_parquet: pathlib.Path,
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

    intersect_query = intersect_template.format(spatial_filter=phase1_filter)
    log.debug("Phase 1 query (after filter):\n%s", intersect_query)

    if explain:
        log_plan(sd, intersect_query)
    intersect_result = _compute_overlap(
        _normalise_arrow(cast(pa.Table, sd.sql(intersect_query).to_arrow_table())),
        max_d,
    )
    log.info("Phase 1: %d intersect matches", len(intersect_result))

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
    log.info("Phase 2 RHS slice: %d rows loaded", len(phase2_rhs_slice))

    if phase1_parts:
        # Remove phase 1 matches from phase 2 slice
        matched_ids: set = set(intersect_result.column(id_col).to_pylist())
        log.debug(f"Matched IDs from Phase 1: {matched_ids}")
        phase2_rhs_slice = phase2_rhs_slice.filter(
            pa.array(
                [
                    rhs_id not in matched_ids
                    for rhs_id in phase2_rhs_slice.column(id_col).to_pylist()
                ]
            )
        )
        log.info(
            "Phase 2 unmatched RHS rows after dropping intersect matches: %d",
            len(phase2_rhs_slice),
        )

    if len(phase2_rhs_slice):
        # Chunk the unmatched phase 2 RHS so each sub-query sees only the USRN row groups
        # that overlap that small area — avoids scanning whole area again.
        _ROWS_PER_CHUNK = 5_000
        n_chunks = max(
            1, (len(phase2_rhs_slice) + _ROWS_PER_CHUNK - 1) // _ROWS_PER_CHUNK
        )
        non_geom = ", ".join(
            f'"{c}"' for c in phase2_rhs_slice.schema.names if c != "geometry"
        )
        log.info(
            "Phase 2 ST_Intersects (buffered): %d unmatched rows → %d chunks",
            len(phase2_rhs_slice),
            n_chunks,
        )

        for i in range(n_chunks):
            sub = phase2_rhs_slice.slice(i * _ROWS_PER_CHUNK, _ROWS_PER_CHUNK)
            if not len(sub):
                continue

            # Derive sub-chunk spatial envelope from the bbox struct column
            bbox_col = sub.column("bbox")
            xmin_c = pc.min(pc.struct_field(bbox_col, "xmin")).as_py()
            ymin_c = pc.min(pc.struct_field(bbox_col, "ymin")).as_py()
            xmax_c = pc.max(pc.struct_field(bbox_col, "xmax")).as_py()
            ymax_c = pc.max(pc.struct_field(bbox_col, "ymax")).as_py()

            sd.create_data_frame(sub).to_view("rhs_raw", overwrite=True)
            sd.sql(
                f"SELECT {non_geom},"
                f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
            ).to_view(rhs_view, overwrite=True)

            # Exact sub-chunk envelope — buffered geometry already covers the corridor
            wkt = (
                f"POLYGON(({xmin_c} {ymin_c},{xmax_c} {ymin_c},"
                f"{xmax_c} {ymax_c},{xmin_c} {ymax_c},{xmin_c} {ymin_c}))"
            )
            usrn_filter = f"AND ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"
            phase2_query = phase2_template.format(spatial_filter=usrn_filter)
            if explain and i == 0:
                log_plan(sd, phase2_query)

            phase2_result = _normalise_arrow(
                cast(pa.Table, sd.sql(phase2_query).to_arrow_table())
            )
            log.debug(
                "Phase 2 chunk %d/%d (%d rows): %d candidates",
                i + 1,
                n_chunks,
                len(sub),
                len(phase2_result),
            )
            if len(phase2_result):
                filtered = _overlap_post_filter(
                    phase2_result, id_col, max_d, overlap_threshold
                )
                if len(filtered):
                    phase2_parts.append(filtered)

    if phase2_parts:
        phase2_table = pa.concat_tables(phase2_parts)
        log.info("Phase 2 after overlap filter: %d matches total", len(phase2_table))
        # Remove phase 2 matches from phase 3 slice
        phase2_matched_ids: set = set(phase2_table.column(id_col).to_pylist())
        phase3_rhs_slice = phase2_rhs_slice.filter(
            pa.array(
                [
                    rhs_id not in phase2_matched_ids
                    for rhs_id in phase2_rhs_slice.column(id_col).to_pylist()
                ]
            )
        )
    else:
        # If phase 2 matched nothing then reuse the entire phase 2 slice
        # for phase 3
        phase3_rhs_slice = phase2_rhs_slice

    # Phase 3: ST_DWithin nearest USRN for features still unmatched after Phase 2
    # Do the same 5,000 row chunks
    # Usually won't be needed
    if len(phase3_rhs_slice):
        log.info(
            "Phase 3 — ST_DWithin nearest on %d still-unmatched RHS rows...",
            len(phase3_rhs_slice),
        )
        _ROWS_PER_CHUNK_P3 = 5_000
        n_chunks_p3 = max(
            1, (len(phase3_rhs_slice) + _ROWS_PER_CHUNK_P3 - 1) // _ROWS_PER_CHUNK_P3
        )
        non_geom_p3 = ", ".join(
            f'"{c}"' for c in phase3_rhs_slice.schema.names if c != "geometry"
        )
        for j in range(n_chunks_p3):
            sub = phase3_rhs_slice.slice(j * _ROWS_PER_CHUNK_P3, _ROWS_PER_CHUNK_P3)
            if not len(sub):
                continue
            bbox_col = sub.column("bbox")
            xmin_c = pc.min(pc.struct_field(bbox_col, "xmin")).as_py()
            ymin_c = pc.min(pc.struct_field(bbox_col, "ymin")).as_py()
            xmax_c = pc.max(pc.struct_field(bbox_col, "xmax")).as_py()
            ymax_c = pc.max(pc.struct_field(bbox_col, "ymax")).as_py()

            sd.create_data_frame(sub).to_view("rhs_raw", overwrite=True)
            sd.sql(
                f"SELECT {non_geom_p3},"
                f" ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry FROM rhs_raw"
            ).to_view(rhs_view, overwrite=True)

            wkt = (
                f"POLYGON(({xmin_c} {ymin_c},{xmax_c} {ymin_c},"
                f"{xmax_c} {ymax_c},{xmin_c} {ymax_c},{xmin_c} {ymin_c}))"
            )
            usrn_filter = f"AND ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"
            phase3_query = phase3_template.format(spatial_filter=usrn_filter)
            if explain and j == 0:
                log_plan(sd, phase3_query)
            phase3_raw = _normalise_arrow(
                cast(pa.Table, sd.sql(phase3_query).to_arrow_table())
            )
            if len(phase3_raw):
                deduped = _phase3_nearest_dedup(phase3_raw, id_col)
                if len(deduped):
                    phase3_parts.append(deduped)

        if phase3_parts:
            phase3_table = pa.concat_tables(phase3_parts)
            log.info("Phase 3 nearest: %d matches", len(phase3_table))

    all_parts = phase1_parts + phase2_parts + phase3_parts
    return pa.concat_tables(all_parts) if all_parts else pa.table({})


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------

_OVERLAP_EXPR = """
    COALESCE(
        ST_Length(
            ST_Intersection(
                ST_Buffer(ST_GeomFromWKB(_u_geom), {max_d}),
                ST_GeomFromWKB(_s_geom)
            )
        ) / NULLIF(GREATEST(ST_Length(ST_GeomFromWKB(_s_geom)), {denom}), 0),
        0.0
    )
"""


def _compute_overlap(table: pa.Table, max_d: float) -> pa.Table:
    """Add ``overlap_length_pct`` and ``match_phase=1`` to Phase 1 results — all pairs kept."""
    con = duckdb.connect()
    con.execute("LOAD spatial")
    con.register("t", table)
    expr = _OVERLAP_EXPR.format(max_d=max_d, denom=2 * max_d)
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
    """
    con = duckdb.connect()
    con.execute("LOAD spatial")
    con.register("t", table)
    expr = _OVERLAP_EXPR.format(max_d=max_d, denom=2 * max_d)
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
    log.info(
        "Overlap post-filter: %d → %d rows (best corridor match per RHS feature kept)",
        len(table),
        len(result),
    )
    return result.append_column(
        "match_phase", pa.array([2] * len(result), type=pa.int8())
    )


def _phase3_nearest_dedup(table: pa.Table, id_col: str) -> pa.Table:
    """Pick the single closest USRN per unmatched RHS feature — Phase 3 nearest fallback."""
    if not len(table):
        return table
    con = duckdb.connect()
    con.register("t", table)
    result = con.execute(f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY "{id_col}" ORDER BY distance_m) AS _rn
            FROM t
        )
        SELECT * EXCLUDE (_rn)
        FROM ranked
        WHERE _rn = 1
    """).fetch_arrow_table()
    return result.append_column(
        "match_phase", pa.array([3] * len(result), type=pa.int8())
    )


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


@register("polygon")
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


@register("point")
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


@register("line")
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

    intersect_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {col_fragment},
                ST_Distance(u.geometry, s.geometry) AS distance_m,
                TRUE AS is_intersection,
                ST_AsWKB(u.geometry) AS _u_geom,
                ST_AsWKB(s.geometry) AS _s_geom
            FROM usrns AS u
            JOIN {rhs_view} AS s ON ST_Intersects(u.geometry, s.geometry)
            WHERE TRUE
            {{spatial_filter}}
            ORDER BY u.usrn, distance_m
        """
    buffered_template: str = f"""
            SELECT
                u.usrn,
                u.street_type
                {col_fragment},
                ST_Distance(u.geometry_line, s.geometry) AS distance_m,
                FALSE AS is_intersection,
                ST_AsWKB(u.geometry_line) AS _u_geom,
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

    return execute_line_join(
        sd,
        usrn_parquet,
        rhs_config.parquet_path,
        rhs_view,
        intersect_template,
        buffered_template,
        phase3_template,
        id_col=rhs_id_col,
        max_d=distance_m,
        mode=mode,
        usrn_line_parquet=usrn_line_parquet,
        explain=explain,
        output_path=output_path,
        overlap_threshold=overlap_threshold,
    )


# ---------------------------------------------------------------------------
# SQL fragment helpers
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
