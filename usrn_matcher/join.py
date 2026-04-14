import logging
import pathlib
from collections.abc import Sequence
from typing import TypeAlias

import pyarrow.parquet as pq
from sedonadb.context import DataFrame, SedonaContext

from .config import DatasetConfig
from .logger import get_logger

BBox: TypeAlias = Sequence[float]

log: logging.Logger = get_logger()


def _bbox_filter(bbox: BBox | None) -> str:
    """Return a WHERE clause restricting both sides to the given EPSG:27700 bbox.

    Filtering both ``u`` (USRNs) and ``s`` (RHS) lets Sedona prune row groups
    on both parquet files.

    Any RHS feature that doesn't overlap the bbox can't possibly intersect a USRN inside it!

    This allows for the Lidl spatial indexing to work.
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
        f"WHERE ST_Intersects(u.geometry, {bbox_geom})"
        f" AND ST_Intersects(s.geometry, {bbox_geom})"
    )


def _bbox_wkt(bbox: BBox | None) -> str | None:
    """Return the bbox as a WKT geometry string for clipping, or None.

    _bbox_filter is for pruning — it goes in the WHERE clause and touches both scans.

    _bbox_wkt is for clipping — it's used inside ST_Intersection(u.geometry, s.geometry, bbox_wkt)

    It trims USRN lines at the bbox boundary so we don't get long roads extending
    outside the area of interest in the output.
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
    rhs_pa_schema: pq.ParquetSchema = pq.read_schema(str(rhs_config.parquet_path))
    auto_cols: list[str] = [
        name for name in rhs_pa_schema.names if name not in ("geometry", "bbox")
    ]
    return ", " + ", ".join(f's."{c}"' for c in auto_cols)


def run_intersect_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    bbox: BBox | None = None,
    explain: bool = False,
    include_rhs_geometry: bool = False,
) -> DataFrame:
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
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry`` in the
        SELECT list. Used by the DTF export step, which encodes the matched RHS
        feature geometry (not the USRN line) into type 67 coordinate records.
    """
    rhs_view: str = rhs_config.name

    rhs_df = sd.read_parquet(str(rhs_config.parquet_path))
    usrn_df = sd.read_parquet(str(usrn_parquet))

    rhs_df.to_view(rhs_view, overwrite=True)
    usrn_df.to_view("usrns", overwrite=True)

    log.info("RHS (%s) schema: %s", rhs_view, rhs_df.schema)
    log.info("USRN schema: %s", usrn_df.schema)
    log.info("RHS (%s) count: %d", rhs_view, rhs_df.count())
    log.info("USRN count: %d", usrn_df.count())

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

    bbox_filter: str = _bbox_filter(bbox)
    bbox_wkt: str | None = _bbox_wkt(bbox)

    # When a bbox is supplied, clip the intersection to it so long USRNs that
    # extend outside the area of interest are trimmed at the boundary.
    intersection_expr: str = (
        f"ST_Intersection(ST_Intersection(u.geometry, s.geometry), {bbox_wkt})"
        if bbox_wkt
        else "ST_Intersection(u.geometry, s.geometry)"
    )

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
            u.street_type,
            {intersection_expr} AS geometry
            {col_fragment}
            {rhs_geom_fragment}
        FROM usrns AS u
        JOIN {rhs_view} AS s
          ON ST_Intersects(u.geometry, s.geometry)
        {bbox_filter}
        ORDER BY u.usrn
    """

    if explain:
        log.info("Query plan (with execution metrics):")
        plan = sd.sql(f"EXPLAIN ANALYZE {query}")
        plan.show(width=400)

    log.info("Running spatial join (usrns × %s)...", rhs_view)
    return sd.sql(query)


def _bbox_nearest_filters(bbox: BBox | None, distance_m: float) -> tuple[str, str]:
    """Return a combined WHERE clause that prunes both sides of a nearest join.

    USRNs (``u``) are filtered to the exact bbox (same as the intersect join).
    Points (``s``) are filtered to the bbox expanded by ``distance_m`` so that
    stops just outside the boundary can still match a USRN inside it.

    Sedona pushes each predicate to its respective parquet scan, pruning row
    groups on both files.

    Returns an empty string when ``bbox`` is ``None``.
    """
    if bbox is None:
        return "", ""
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
    return f"WHERE {usrn_pred} AND {point_pred}", ""


def run_nearest_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    rhs_config: DatasetConfig,
    distance_m: float = 50.0,
    bbox: BBox | None = None,
    explain: bool = False,
    include_rhs_geometry: bool = False,
) -> DataFrame:
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
        are considered candidates. Default is 50 m.
    bbox:
        Optional ``[xmin, ymin, xmax, ymax]`` in EPSG:27700 to restrict which
        points are matched. When ``None`` all points are matched.
    explain:
        If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
    include_rhs_geometry:
        If ``True``, include ``ST_AsWKB(s.geometry) AS rhs_geometry`` in the
        SELECT list. Used by the DTF export step, which stores the matched RHS
        point geometry as WKT in the paired type 67a record.

    Returns
    -------
    One row per USRN–point pair within ``distance_m``, ordered by
    ``usrn, distance_m``. Columns: ``usrn``, ``street_type``, all RHS
    ``columns`` (or auto-discovered), ``distance_m``.
    """
    rhs_view: str = rhs_config.name

    rhs_df = sd.read_parquet(str(rhs_config.parquet_path))
    usrn_df = sd.read_parquet(str(usrn_parquet))

    rhs_df.to_view(rhs_view, overwrite=True)
    usrn_df.to_view("usrns", overwrite=True)

    log.info("RHS (%s) schema: %s", rhs_view, rhs_df.schema)
    log.info("USRN schema: %s", usrn_df.schema)
    log.info("RHS (%s) count: %d", rhs_view, rhs_df.count())
    log.info("USRN count: %d", usrn_df.count())

    # We intentionally do not set sedona.spatial_join.execution_mode — see run_intersect_join.

    # Sedona assigns build/probe sides by cardinality (should_swap_join_order in physical_planner.rs):
    # smaller row count → build side (R-tree + PreparedGeometry), larger → probe side.
    # For stops (434K) vs USRNs (1.76M): stops = build, USRNs = probe.
    # This will flip for any RHS dataset larger than the USRN count.

    # Apply bbox to both sides so Sedona can prune row groups on both parquets:
    #   - USRNs: exact bbox (same as run_intersect_join)
    #   - Points: bbox expanded by distance_m to include points near the boundary
    bbox_filter: str
    bbox_filter, _ = _bbox_nearest_filters(bbox, distance_m)

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
    bbox_wkt: str | None = _bbox_wkt(bbox)

    # Clip the USRN geometry to the bbox so long streets don't extend outside
    # the area of interest — mirrors the ST_Intersection clipping in run_intersect_join.
    # ST_Intersection also avoids the raw WkbView segfault in to_arrow_table(), so
    # ST_AsWKB is only needed for the unclipped (no-bbox) case.
    geometry_expr: str = (
        f"ST_Intersection(u.geometry, {bbox_wkt})"
        if bbox_wkt
        else "ST_AsWKB(u.geometry)"
    )
    rhs_geom_fragment: str = (
        ", ST_AsWKB(s.geometry) AS rhs_geometry" if include_rhs_geometry else ""
    )

    query: str = f"""
        SELECT
            u.usrn,
            u.street_type,
            {geometry_expr} AS geometry
            {col_fragment},
            ST_Distance(u.geometry, s.geometry) AS distance_m
            {rhs_geom_fragment}
        FROM usrns AS u
        JOIN {rhs_view} AS s
          ON ST_DWithin(u.geometry, s.geometry, {distance_m})
        {bbox_filter}
        ORDER BY u.usrn, distance_m
    """

    if explain:
        log.info("Query plan (with execution metrics):")
        plan = sd.sql(f"EXPLAIN ANALYZE {query}")
        plan.show(width=400)

    log.info(
        "Running nearest-USRN join (%s → usrns, radius=%.0fm)...", rhs_view, distance_m
    )
    return sd.sql(query)
