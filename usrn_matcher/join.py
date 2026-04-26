import functools
import logging
import pathlib
from typing import Any, Callable, Literal, Protocol, TypeVar, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq
from sedonadb.context import SedonaContext

from .config import BBox, DatasetConfig
from .explain import log_plan
from .logger import get_logger

log: logging.Logger = get_logger()

GeometryMode = Literal["none", "usrn", "clip", "rhs"]


# Define a proper JoinFunction type
@runtime_checkable
class JoinFn(Protocol):
    """Contract every join implementation must satisfy."""

    def __call__(
        self,
        sd: SedonaContext,
        usrn_parquet: pathlib.Path,
        rhs_config: DatasetConfig,
        *,
        bbox: BBox | None = ...,
        explain: bool = ...,
        include_rhs_geometry: bool = ...,
        usrn_batches: int = ...,
        **kwargs: Any,
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


def execute_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    query: str,
    explain: bool,
    usrn_batches: int,
) -> pa.Table:
    """Register the USRN view and run a pre-built spatial join query.

    When ``usrn_batches == 1`` the full parquet is registered as a single Sedona
    scan.

    When ``usrn_batches > 1`` the parquet row groups are divided into
    equal-sized chunks; each chunk is loaded into an in-memory Arrow table and
    registered via ``create_data_frame`` — no temp files.

    Results are concatenated in row-group order.
    """
    if usrn_batches <= 1:
        sd.read_parquet(str(usrn_parquet)).to_view("usrns", overwrite=True)
        filled: str = query.format(batch_filter="")
        if explain:
            log_plan(sd, filled)
        return sd.sql(filled).to_arrow_table()

    usrn_pf: pq.ParquetFile = pq.ParquetFile(str(usrn_parquet))
    n_row_groups: int = usrn_pf.metadata.num_row_groups
    rgs_per_batch: int = max(1, (n_row_groups + usrn_batches - 1) // usrn_batches)
    usrn_slices: list[list[int]] = [
        list(range(start, min(start + rgs_per_batch, n_row_groups)))
        for start in range(0, n_row_groups, rgs_per_batch)
    ]
    n_batches: int = len(usrn_slices)
    log.info(
        "Batching USRN parquet: %d row groups → %d batches", n_row_groups, n_batches
    )

    # Discover bbox leaf column indices from parquet schema once.
    first_rg = usrn_pf.metadata.row_group(0)
    col_path_to_idx: dict[Any, int] = {
        first_rg.column(i).path_in_schema: i for i in range(first_rg.num_columns)
    }
    bbox_col_idx: dict[str, int] = {
        f: col_path_to_idx[f"bbox.{f}"] for f in ("xmin", "ymin", "xmax", "ymax")
    }

    batch_results: list[pa.Table] = []
    for i, usrn_slice in enumerate(usrn_slices):
        usrn_batch: pa.Table = usrn_pf.read_row_groups(usrn_slice)

        # Derive batch envelope from row-group statistics — no extra data scan.
        slice_rg_metas = [usrn_pf.metadata.row_group(rg) for rg in usrn_slice]
        xmin: float = min(
            rg.column(bbox_col_idx["xmin"]).statistics.min for rg in slice_rg_metas
        )
        ymin: float = min(
            rg.column(bbox_col_idx["ymin"]).statistics.min for rg in slice_rg_metas
        )
        xmax: float = max(
            rg.column(bbox_col_idx["xmax"]).statistics.max for rg in slice_rg_metas
        )
        ymax: float = max(
            rg.column(bbox_col_idx["ymax"]).statistics.max for rg in slice_rg_metas
        )
        bbox_wkt = f"POLYGON(({xmin} {ymin},{xmax} {ymin},{xmax} {ymax},{xmin} {ymax},{xmin} {ymin}))"
        batch_filter = f"AND ST_Intersects(s.geometry, ST_SetSRID(ST_GeomFromWKT('{bbox_wkt}'), 27700))"
        batch_query = query.format(batch_filter=batch_filter)

        sd.create_data_frame(usrn_batch).to_view("usrns_raw", overwrite=True)
        sd.sql(
            "SELECT usrn, street_type,"
            " ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry"
            " FROM usrns_raw"
        ).to_view("usrns", overwrite=True)
        log.info(
            "Batch %d/%d: %d row groups, %d rows — bbox xmin=%.0f ymin=%.0f xmax=%.0f ymax=%.0f",
            i + 1,
            n_batches,
            len(usrn_slice),
            len(usrn_batch),
            xmin,
            ymin,
            xmax,
            ymax,
        )
        if explain and i == 0:
            log_plan(sd, batch_query)
        batch_results.append(sd.sql(batch_query).to_arrow_table())

    return pa.concat_tables(batch_results)


@register("intersect")
def run_intersect_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    bbox: BBox | None = None,
    explain: bool = False,
    include_rhs_geometry: bool = False,
    usrn_batches: int = 1,
    geometry: GeometryMode = "none",
    **_: Any,
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
    bbox:
        Optional ``[xmin, ymin, xmax, ymax]`` in EPSG:27700. When ``None``
        a full dataset join is executed with no spatial pre-filter.
    explain:
        If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
    include_rhs_geometry:
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry`` as an
        additional column. Used by the DTF export step.
    usrn_batches:
        Split the USRN parquet into this many batches and concatenate results.
        ``1`` loads the full file as a single Sedona scan. Use ``4`` or more
        for national (no-bbox) joins to avoid memory spikes.
    geometry:
        Controls the primary ``geometry`` column in the output.
        ``"none"`` (default) — omit geometry entirely (fastest; attribute-only output).
        ``"usrn"`` — full USRN line (``ST_AsWKB``), clipped to bbox when one is supplied.
        ``"clip"`` — USRN geometry clipped to the matched RHS polygon
        (``ST_Intersection``). Slower; useful when the segmented line is needed.
        ``"rhs"`` — the matched RHS feature geometry instead of the USRN line.
    """
    rhs_view: str = rhs_config.name

    rhs_df = sd.read_parquet(str(rhs_config.parquet_path))
    rhs_df.to_view(rhs_view, overwrite=True)

    log.info("RHS (%s) schema: %s", rhs_view, rhs_df.schema)
    log.info("RHS (%s) count: %d", rhs_view, rhs_df.count())
    log.info("USRN schema: %s", pq.read_schema(str(usrn_parquet)))
    log.info("USRN count: %d", pq.read_metadata(str(usrn_parquet)).num_rows)

    # IMPORTANT: We intentionally do not set sedona.spatial_join.execution_mode.
    # The default is Speculative(N), which samples the first N probe-side geometries
    # at runtime and picks the best mode (prepare_build, prepare_probe, or prepare_none)
    # based on actual geometry complexity.
    #
    # Hardcoding prepare_build overrides this and
    # can be worse — e.g. if probe-side geometries are complex, prepare_probe wins.
    #
    # Sedona also handles build/probe side assignment automatically via should_swap_join_order(),
    # which puts the smaller table on the build side based on cardinality estimates.

    bbox_filter: str = _bbox_pruner(bbox)
    bbox_clip: str | None = _bbox_clipper(bbox)

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

    if bbox:
        log.info("Applying bbox filter: xmin=%s ymin=%s xmax=%s ymax=%s", *bbox)
    else:
        log.info("No bbox supplied — running full dataset join.")

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
        {bbox_filter}
        {{batch_filter}}
        ORDER BY u.usrn
    """

    log.info("Running spatial join (usrns × %s)...", rhs_view)
    return execute_join(sd, usrn_parquet, query, explain, usrn_batches)


@register("nearest")
def run_nearest_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    *,
    distance_m: float = 10.0,
    bbox: BBox | None = None,
    explain: bool = False,
    include_rhs_geometry: bool = False,
    usrn_batches: int = 1,
    geometry: GeometryMode = "none",
    **_: Any,
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
    distance_m:
        Search radius in metres (EPSG:27700). Only USRNs within this distance
        are considered candidates. Default is 10 m.
    bbox:
        Optional ``[xmin, ymin, xmax, ymax]`` in EPSG:27700 to restrict which
        points are matched. When ``None`` all points are matched.
    explain:
        If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
    include_rhs_geometry:
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry`` as an
        additional column. Used by the DTF export step.
    usrn_batches:
        Split the USRN parquet into this many batches and concatenate results.
        ``1`` loads the full file as a single Sedona scan. Use ``4`` or more
        for national (no-bbox) joins to avoid memory spikes.
    geometry:
        Controls the primary ``geometry`` column in the output.
        ``"none"`` (default) — omit geometry entirely (fastest; attribute-only output).
        ``"usrn"`` or ``"clip"`` — full USRN line (no meaningful clip for nearest).
        ``"rhs"`` — the matched RHS point geometry.
    """
    rhs_view: str = rhs_config.name

    rhs_df = sd.read_parquet(str(rhs_config.parquet_path))
    rhs_df.to_view(rhs_view, overwrite=True)

    log.info("RHS (%s) schema: %s", rhs_view, rhs_df.schema)
    log.info("RHS (%s) count: %d", rhs_view, rhs_df.count())
    log.info("USRN schema: %s", pq.read_schema(str(usrn_parquet)))
    log.info("USRN count: %d", pq.read_metadata(str(usrn_parquet)).num_rows)

    # We intentionally do not set sedona.spatial_join.execution_mode — see run_intersect_join.

    # Sedona assigns build/probe sides by cardinality (should_swap_join_order in physical_planner.rs):
    # smaller row count → build side (R-tree + PreparedGeometry), larger → probe side.
    # For stops (434K) vs USRNs (1.76M): stops = build, USRNs = probe.
    # This will flip for any RHS dataset larger than the USRN count.

    # Apply bbox to both sides so Sedona can prune row groups on both parquets:
    #   - USRNs: exact bbox (same as run_intersect_join)
    #   - Points: bbox expanded by distance_m to include points near the boundary
    bbox_filter: str = _bbox_nearest_filters(bbox, distance_m)

    # Rewrite as a combined WHERE instead of separate filters — Sedona pushes
    # each predicate down to its respective scan, pruning both parquets.
    if bbox:
        log.info(
            "Applying bbox filter (USRNs exact, points expanded by %.0fm): "
            "xmin=%s ymin=%s xmax=%s ymax=%s",
            distance_m,
            *bbox,
        )
    else:
        log.info("No bbox supplied — matching all points.")

    col_fragment: str = _col_fragment(rhs_config)
    bbox_clip: str | None = _bbox_clipper(bbox)

    match geometry:
        case "none":
            geometry_select = ""
        case "rhs":
            geometry_select = ",\n            ST_AsWKB(s.geometry) AS geometry"
        case "usrn" | "clip":
            # ST_Intersection also avoids the raw WkbView segfault in to_arrow_table(), so
            # ST_AsWKB is only needed for the unclipped (no-bbox) case.
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
        {bbox_filter}
        {{batch_filter}}
        ORDER BY u.usrn, distance_m
    """

    log.info(
        "Running nearest-USRN join (%s → usrns, radius=%.0fm)...", rhs_view, distance_m
    )
    return execute_join(sd, usrn_parquet, query, explain, usrn_batches)


@functools.lru_cache(maxsize=None)
def _read_auto_cols(parquet_path: str) -> tuple[str, ...]:
    """Return non-geometry column names from a parquet footer — cached per path."""
    schema = pq.read_schema(parquet_path)
    return tuple(name for name in schema.names if name not in ("geometry", "bbox"))


def _bbox_pruner(bbox: BBox | None) -> str:
    """Return AND conditions that prune both parquet scans to the given EPSG:27700 bbox.

    Both ``u`` (USRNs) and ``s`` (RHS) are filtered so Sedona can skip row groups
    on both files. Any RHS feature outside the bbox cannot intersect a USRN inside it.
    Returns an empty string when bbox is None; the caller must supply a WHERE TRUE base.
    """
    if bbox is None:
        return ""
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    xmin, ymin, xmax, ymax = bbox
    wkt: str = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    bbox_geom: str = f"ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700)"
    return (
        f"AND ST_Intersects(u.geometry, {bbox_geom})"
        f" AND ST_Intersects(s.geometry, {bbox_geom})"
    )


def _bbox_clipper(bbox: BBox | None) -> str | None:
    """Return the bbox as a Sedona geometry expression for use inside ST_Intersection, or None.

    Clips USRN lines at the bbox boundary so long roads don't extend outside the
    area of interest. Distinct from ``_bbox_pruner``, which goes in the WHERE clause
    to skip row groups — this trims the output geometry itself.
    """
    if bbox is None:
        return None
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    xmin, ymin, xmax, ymax = bbox
    return f"ST_SetSRID(ST_GeomFromWKT('POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))'), 27700)"


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


def _bbox_nearest_filters(bbox: BBox | None, distance_m: float) -> str:
    """Return a combined WHERE clause that prunes both sides of a nearest join.

    USRNs (``u``) are filtered to the exact bbox (same as the intersect join).
    Points (``s``) are filtered to the bbox expanded by ``distance_m`` so that
    stops just outside the boundary can still match a USRN inside it.

    Sedona pushes each predicate to its respective parquet scan, pruning row
    groups on both files.

    Returns an empty string when ``bbox`` is ``None``.
    """
    if bbox is None:
        return ""
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    xmin, ymin, xmax, ymax = bbox

    def _pred(alias: str, x0: float, y0: float, x1: float, y1: float) -> str:
        wkt: str = f"POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))"
        return f"ST_Intersects({alias}.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"

    usrn_pred: str = _pred("u", xmin, ymin, xmax, ymax)
    point_pred: str = _pred(
        "s",
        xmin - distance_m,
        ymin - distance_m,
        xmax + distance_m,
        ymax + distance_m,
    )
    return f"AND {usrn_pred} AND {point_pred}"
