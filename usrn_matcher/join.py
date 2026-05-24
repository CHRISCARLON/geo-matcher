import functools
import logging
import pathlib
import tempfile
from dataclasses import dataclass
from typing import Callable, Literal, Protocol, TypeVar, cast, runtime_checkable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from sedonadb.context import SedonaContext

from .config import BBox, DatasetConfig
from .explain import log_plan
from .logger import get_logger

log: logging.Logger = get_logger()

GeometryMode = Literal["none", "usrn", "clip", "rhs"]


# ---------------------------------------------------------------------------
# Analysis mode — discriminated union
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FilteredMode:
    """Spatially filtered join — runs as a single SQL query scoped to *bbox*.

    Both the USRN and RHS parquets are registered as Sedona views; a single SQL
    query runs with ``ST_Intersects`` predicates against the bbox polygon in the
    WHERE clause, letting Sedona skip non-overlapping row groups via GeoParquet
    1.1 covering metadata on both sides. No Python-level data loading occurs.
    """

    bbox: BBox


@dataclass(frozen=True)
class NationalMode:
    """Full national join — splits the RHS parquet into chunks.

    The USRN parquet is registered as a persistent Sedona view once so its
    GeoParquet 1.1 covering metadata drives row-group pruning. The RHS parquet
    is split into *n_chunks* in-memory slices; for each chunk, a
    ``ST_Intersects(u.geometry, chunk_envelope)`` predicate lets Sedona skip
    the ~80 % of USRN row groups that don't overlap. Results are written
    directly to a ``ParquetWriter`` — at most one chunk's matches are in memory
    at a time.
    """

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
        include_rhs_geometry: bool = ...,
        distance_m: float = ...,
        geometry: GeometryMode = ...,
        rhs_id_col: str | None = ...,
        output_path: pathlib.Path | None = ...,
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


def _configure_session(sd: SedonaContext, target_partitions: int = 4) -> None:
    """Apply execution-time tuning to a freshly created SedonaContext.

    Call once immediately after sedonadb.connect(), before any other query.

    target_partitions caps DataFusion's RoundRobinBatch fan-out — the dominant
    source of CPU saturation on national line joins (default 4 ≈ 36 % of an
    11-core machine vs 100 % uncapped).

    repartition_probe_side=false: the probe stream is already spatially
    pre-filtered by chunk envelope; re-shuffling it into N partitions before
    the spatial join adds a memcpy round-trip without improving load balance
    on hilbert-sorted input.

    concurrent_build_side_collection=false: the RHS build side per chunk is
    ~50k rows / ~24 MB; parallel collection overhead exceeds any gain at
    that size.
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
    return {f: path_to_idx[f"bbox.{f}"] for f in ("xmin", "ymin", "xmax", "ymax")}


def _slice_envelope(
    pf: pq.ParquetFile, rg_slice: list[int], bbox_idx: dict[str, int]
) -> BBox:
    """Derive the spatial envelope of a set of row groups from their column statistics."""
    rgs = [pf.metadata.row_group(i) for i in rg_slice]
    return (
        min(rg.column(bbox_idx["xmin"]).statistics.min for rg in rgs),
        min(rg.column(bbox_idx["ymin"]).statistics.min for rg in rgs),
        max(rg.column(bbox_idx["xmax"]).statistics.max for rg in rgs),
        max(rg.column(bbox_idx["ymax"]).statistics.max for rg in rgs),
    )


# ---------------------------------------------------------------------------
# Core executor
# ---------------------------------------------------------------------------


def execute_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_parquet: pathlib.Path,
    rhs_view: str,
    query_template: str,
    mode: AnalysisMode,
    filter_fn: Callable[[BBox], str],
    explain: bool = False,
    output_path: pathlib.Path | None = None,
    usrn_expand_m: float = 0.0,
) -> pa.Table:
    """Run the spatial join according to selected *mode*.

    ``filter_fn`` accepts a BBox and returns AND-clause predicates for the
    ``{spatial_filter}`` placeholder in *query_template*.

    **FilteredMode**: both parquets are registered as Sedona views; a single
    SQL query runs with ``ST_Intersects`` predicates against the bbox polygon,
    letting Sedona skip non-overlapping row groups on both sides via GeoParquet
    1.1 covering metadata.

    **NationalMode**: the USRN parquet is registered as a persistent Sedona view
    once. The RHS parquet is split into *n_chunks* in-memory slices via PyArrow;
    each chunk's spatial envelope is derived from parquet footer statistics and
    passed as ``{spatial_filter}`` — Sedona skips USRN row groups that don't
    overlap the chunk without loading any USRN data into Python.
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
            query = query_template.format(spatial_filter=spatial_filter)
            if explain:
                log_plan(sd, query)
            log.info("Filtered join (usrns × %s)...", rhs_view)
            return cast(pa.Table, sd.sql(query).to_arrow_table())

        case NationalMode(n_chunks=n_chunks):
            if output_path is not None:
                _stream_rhs_chunks(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    query_template,
                    usrn_expand_m,
                    explain,
                    output_path,
                    n_chunks=n_chunks,
                )
                return pa.table({})
            with tempfile.TemporaryDirectory() as _tmp:
                _path = pathlib.Path(_tmp) / "stream.parquet"
                _stream_rhs_chunks(
                    sd,
                    usrn_parquet,
                    rhs_parquet,
                    rhs_view,
                    query_template,
                    usrn_expand_m,
                    explain,
                    _path,
                    n_chunks=n_chunks,
                )
                return pq.read_table(str(_path)) if _path.exists() else pa.table({})


# ---------------------------------------------------------------------------
# RHS-chunked streaming (NationalMode + output_path)
# ---------------------------------------------------------------------------


def _stream_rhs_chunks(
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
    """Split the RHS parquet into chunks and stream join results directly to *output_path*.

    USRN is registered as a persistent Sedona parquet view so GeoParquet 1.1
    covering metadata drives row-group pruning.

    Each RHS chunk's spatial envelope (from parquet column statistics) is expanded by *usrn_expand_m*
    and used as a ``ST_Intersects`` predicate so Sedona skips the ~80% of
    USRN row groups that don't overlap the chunk. Results are written to a
    ``pq.ParquetWriter`` incrementally — at most one chunk's matches in memory
    at a time.
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
        for i, rhs_slice in enumerate(slices):
            chunk = rhs_pf.read_row_groups(rhs_slice)
            envelope = _slice_envelope(rhs_pf, rhs_slice, bbox_idx)

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
            result = cast(pa.Table, sd.sql(query).to_arrow_table())
            if len(result):
                if writer is None:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(str(output_path), result.schema)
                writer.write_table(result)
                log.info(
                    "Chunk %d/%d (%d rhs rgs): %d matches",
                    i + 1,
                    len(slices),
                    len(rhs_slice),
                    len(result),
                )
    finally:
        if writer:
            writer.close()


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------


def _prefer_intersection(table: pa.Table, id_col: str) -> pa.Table:
    """Keep intersecting matches; fall back to the single nearest for unmatched RHS features.

    For each RHS feature (identified by *id_col*):
    - If any row has ``is_intersection=True`` → keep all those rows (may be multiple USRNs).
    - Otherwise → keep the single row with the smallest ``distance_m``.

    The ``is_intersection`` column is preserved in the output as a diagnostic field.
    """
    con = duckdb.connect()
    con.register("t", table)
    result: pa.Table = con.execute(f"""
        WITH ranked AS (
            SELECT *,
                MAX(is_intersection::INT) OVER (PARTITION BY "{id_col}") AS _has_any,
                ROW_NUMBER() OVER (
                    PARTITION BY "{id_col}"
                    ORDER BY is_intersection DESC, distance_m ASC
                ) AS _rn
            FROM t
        )
        SELECT * EXCLUDE (_has_any, _rn)
        FROM ranked
        WHERE is_intersection
           OR (NOT _has_any::BOOL AND _rn = 1)
    """).arrow()
    log.info(
        "Intersection preference: %d → %d rows (touching matches kept; unmatched → nearest only)",
        len(table),
        len(result),
    )
    return result


# ---------------------------------------------------------------------------
# Join implementations
# ---------------------------------------------------------------------------


@register("intersect")
def run_intersect_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: AnalysisMode = _DEFAULT_MODE,
    explain: bool = False,
    include_rhs_geometry: bool = False,
    distance_m: float = 10.0,  # noqa: ARG002
    geometry: GeometryMode = "none",
    rhs_id_col: str | None = None,  # noqa: ARG002
    output_path: pathlib.Path | None = None,
) -> pa.Table:
    """Run a spatial intersection join of USRNs against any right-hand side dataset.

    Parameters
    ----------
    sd:
        Active SedonaContext.
    usrn_parquet:
        Path to the prepared USRN GeoParquet.
    rhs_config:
        Configuration for the right-hand side dataset. Its ``name`` is used as
        the SQL view name and ``columns`` drives the SELECT list. When
        ``columns`` is empty, all columns except ``geometry`` and the internal
        ``bbox`` covering column are selected automatically.
    mode:
        ``FilteredMode(bbox=...)`` — single query scoped to the bbox.
        ``NationalMode(n_chunks=...)`` — RHS-chunked national join (default, 50 chunks).
    explain:
        If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
    include_rhs_geometry:
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry`` as an
        additional column.
    geometry:
        Controls the primary ``geometry`` column in the output.
        ``"none"`` (default) — omit geometry entirely (fastest; attribute-only output).
        ``"usrn"`` — full USRN line (``ST_AsWKB``), clipped to bbox for ``FilteredMode``.
        ``"clip"`` — USRN geometry clipped to the matched RHS polygon.
        ``"rhs"`` — the matched RHS feature geometry instead of the USRN line.
    """
    rhs_view: str = rhs_config.name
    bbox_clip: str | None = _bbox_clipper(mode)

    match mode:
        case FilteredMode(bbox=bbox):
            log.info("Bbox filter: xmin=%s ymin=%s xmax=%s ymax=%s", *bbox)
        case NationalMode():
            log.info("No bbox supplied — running full national join.")

    match geometry:
        case "none":
            geometry_select = ""
        case "rhs":
            geometry_select = ",\n            ST_AsWKB(s.geometry) AS geometry"
        case "clip":
            geom_expr: str = (
                f"ST_Intersection(ST_Intersection(u.geometry, s.geometry), {bbox_clip})"
                if bbox_clip
                else "ST_Intersection(u.geometry, s.geometry)"
            )
            geometry_select = f",\n            {geom_expr} AS geometry"
        case "usrn":
            geom_expr = (
                f"ST_Intersection(u.geometry, {bbox_clip})"
                if bbox_clip
                else "ST_AsWKB(u.geometry)"
            )
            geometry_select = f",\n            {geom_expr} AS geometry"

    col_fragment: str = _col_fragment(rhs_config)
    rhs_geom_fragment: str = (
        ", ST_AsWKB(s.geometry) AS rhs_geometry" if include_rhs_geometry else ""
    )

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {geometry_select}
            {col_fragment}
            {rhs_geom_fragment}
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


@register("nearest")
def run_nearest_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    mode: AnalysisMode = _DEFAULT_MODE,
    distance_m: float = 10.0,
    explain: bool = False,
    include_rhs_geometry: bool = False,
    geometry: GeometryMode = "none",
    rhs_id_col: str | None = None,  # noqa: ARG002
    output_path: pathlib.Path | None = None,
) -> pa.Table:
    """Find the nearest USRN for each point in the RHS dataset.

    Uses a range join (``ST_DWithin``) to find all USRNs within ``distance_m``
    metres of each point, ordered by distance ascending.

    Parameters
    ----------
    sd:
        Active SedonaContext.
    usrn_parquet:
        Path to the prepared USRN GeoParquet.
    rhs_config:
        Configuration for the point dataset. ``columns`` drives the SELECT list;
        empty means auto-discover from the parquet schema.
    mode:
        ``FilteredMode(bbox=...)`` — restrict to a specific area.
        ``NationalMode(n_chunks=...)`` — RHS-chunked national join (default).
    distance_m:
        Search radius in metres (EPSG:27700). Default is 10 m.
    explain:
        If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
    include_rhs_geometry:
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry``.
    geometry:
        ``"none"`` (default) — omit geometry.
        ``"usrn"`` or ``"clip"`` — full USRN line (no meaningful clip for nearest).
        ``"rhs"`` — the matched RHS point geometry.
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
    bbox_clip: str | None = _bbox_clipper(mode)

    match geometry:
        case "none":
            geometry_select = ""
        case "rhs":
            geometry_select = ",\n            ST_AsWKB(s.geometry) AS geometry"
        case "usrn" | "clip":
            # ST_Intersection also avoids the raw WkbView segfault in to_arrow_table(), so
            # ST_AsWKB is only needed for the unclipped (NationalMode) case.
            # No meaningful clip for nearest — "clip" falls back to full USRN line.
            geom_expr: str = (
                f"ST_Intersection(u.geometry, {bbox_clip})"
                if bbox_clip
                else "ST_AsWKB(u.geometry)"
            )
            geometry_select = f",\n            {geom_expr} AS geometry"

    rhs_geom_fragment: str = (
        ", ST_AsWKB(s.geometry) AS rhs_geometry" if include_rhs_geometry else ""
    )

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {geometry_select}
            {col_fragment},
            ST_Distance(u.geometry, s.geometry) AS distance_m
            {rhs_geom_fragment}
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
    explain: bool = False,
    include_rhs_geometry: bool = False,
    geometry: GeometryMode = "none",
    rhs_id_col: str | None = None,
    output_path: pathlib.Path | None = None,
) -> pa.Table:
    """Find USRNs within *distance_m* metres of each linestring in the RHS dataset.

    Uses a range join (``ST_DWithin``) so near-miss road pairs — digitised with
    small positional offsets — are matched even when they don't technically intersect.

    Parameters
    ----------
    sd:
        Active SedonaContext.
    usrn_parquet:
        Path to the prepared USRN GeoParquet.
    rhs_config:
        Configuration for the linestring dataset. ``columns`` drives the SELECT
        list; empty means auto-discover from the parquet schema.
    mode:
        ``FilteredMode(bbox=...)`` — restrict to a specific area.
        ``NationalMode(n_batches=...)`` — match all lines nationally (default).
    distance_m:
        Tolerance in metres (EPSG:27700). Default is 10 m.
    explain:
        If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
    include_rhs_geometry:
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry``.
    geometry:
        ``"none"`` — omit geometry (fastest).
        ``"usrn"`` — full or bbox-clipped USRN line.
        ``"rhs"`` — the matched RHS line geometry.
        ``"clip"`` — portion of each USRN inside the tolerance corridor around the
        matched RHS line (``ST_Intersection(u, ST_Buffer(s, distance_m))``).
    rhs_id_col:
        Column that uniquely identifies each RHS feature. When set, applies
        intersection-preferred matching after all tiles are collected.
        ``None`` (default) returns all pairs within ``distance_m`` unfiltered.
    """
    rhs_view: str = rhs_config.name

    match mode:
        case FilteredMode(bbox=bbox):
            log.info(
                "Bbox filter (USRNs exact, RHS expanded by %.0fm): "
                "xmin=%s ymin=%s xmax=%s ymax=%s",
                distance_m,
                *bbox,
            )
        case NationalMode():
            log.info("No bbox supplied — matching all lines.")

    col_fragment: str = _col_fragment(rhs_config)
    bbox_clip: str | None = _bbox_clipper(mode)

    match geometry:
        case "none":
            geometry_select = ""
        case "rhs":
            geometry_select = ",\n            ST_AsWKB(s.geometry) AS geometry"
        case "usrn":
            geom_expr = (
                f"ST_Intersection(u.geometry, {bbox_clip})"
                if bbox_clip
                else "ST_AsWKB(u.geometry)"
            )
            geometry_select = f",\n            {geom_expr} AS geometry"
        case "clip":
            corridor = f"ST_Buffer(s.geometry, {distance_m})"
            clip_expr = (
                f"ST_Intersection(u.geometry, ST_Intersection({corridor}, {bbox_clip}))"
                if bbox_clip
                else f"ST_Intersection(u.geometry, {corridor})"
            )
            geometry_select = f",\n            {clip_expr} AS geometry"

    rhs_geom_fragment: str = (
        ", ST_AsWKB(s.geometry) AS rhs_geometry" if include_rhs_geometry else ""
    )

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type
            {geometry_select}
            {col_fragment},
            ST_Distance(u.geometry, s.geometry) AS distance_m,
            ST_Intersects(u.geometry, s.geometry) AS is_intersection
            {rhs_geom_fragment}
        FROM usrns AS u
        JOIN {rhs_view} AS s
          ON ST_DWithin(u.geometry, s.geometry, {distance_m})
        WHERE TRUE
        {{spatial_filter}}
        ORDER BY u.usrn, distance_m
    """

    result: pa.Table = execute_join(
        sd,
        usrn_parquet,
        rhs_config.parquet_path,
        rhs_view,
        query,
        mode,
        filter_fn=lambda bbox: _bbox_nearest_filters(bbox, distance_m),
        explain=explain,
        output_path=output_path,
        usrn_expand_m=distance_m,
    )
    if rhs_id_col is not None and len(result):
        result = _prefer_intersection(result, rhs_id_col)
    return result


# ---------------------------------------------------------------------------
# SQL fragment helpers
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=None)
def _read_auto_cols(parquet_path: str) -> tuple[str, ...]:
    """Return non-geometry column names from a parquet footer — cached per path."""
    schema = pq.read_schema(parquet_path)
    return tuple(name for name in schema.names if name not in ("geometry", "bbox"))


def _bbox_clipper(mode: AnalysisMode) -> str | None:
    """Return a Sedona geometry expression for the bbox clip polygon, or None.

    For ``FilteredMode``: clips USRN lines at the bbox boundary so long roads
    don't extend outside the area of interest.
    For ``NationalMode``: returns None (no clipping; full geometries are kept).

    Distinct from ``_bbox_pruner`` which goes in the WHERE clause to skip row
    groups — this trims the output geometry itself.
    """
    match mode:
        case FilteredMode(bbox=bbox):
            xmin, ymin, xmax, ymax = bbox
            return f"ST_SetSRID(ST_GeomFromWKT('POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))'), 27700)"
        case NationalMode():
            return None


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
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    bbox_geom = f"ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700)"
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
