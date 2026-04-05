import pathlib

from sedonadb.context import SedonaContext

from .logger import get_logger

log = get_logger()


def _bbox_filter(bbox: list[float] | None) -> str:
    """Return a WHERE clause restricting to the given EPSG:27700 bounding box, or empty string."""
    if bbox is None:
        return ""
    xmin, ymin, xmax, ymax = bbox
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
    return (
        f"WHERE ST_Intersects(u.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"
    )


def _bbox_wkt(bbox: list[float] | None) -> str | None:
    """Return the bbox as a WKT geometry string for clipping, or None."""
    if bbox is None:
        return None
    xmin, ymin, xmax, ymax = bbox
    return f"ST_SetSRID(ST_GeomFromWKT('POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))'), 27700)"


def run_join(
    sd: SedonaContext,
    usrn_parquet: pathlib.Path,
    soil_parquet: pathlib.Path,
    bbox: list[float] | None = None,
    explain: bool = False,
):
    soil_df = sd.read_parquet(str(soil_parquet))
    usrn_df = sd.read_parquet(str(usrn_parquet))

    soil_df.to_view("soil", overwrite=True)
    usrn_df.to_view("usrns", overwrite=True)

    log.info("Soil schema: %s", soil_df.schema)
    log.info("USRN schema: %s", usrn_df.schema)
    log.info("Soil count: %d", soil_df.count())
    log.info("USRN count: %d", usrn_df.count())

    # Pre-process build-side (soil) geometries once — speeds up the 1.4M probe queries.
    # Try make Sedona trigger the R-Tree indexes for the soil polygones.
    sd.sql("SET sedona.spatial_join.execution_mode TO 'prepare_build'").execute()

    # Smaller chunks
    # TODO: Check that this does help performance
    sd.sql("SET sedona.spatial_join.parallel_refinement_chunk_size TO 4096").execute()

    bbox_filter = _bbox_filter(bbox)
    bbox_wkt = _bbox_wkt(bbox)

    # When a bbox is supplied, clip the intersection to it so long USRNs that
    # extend outside the area of interest are trimmed at the boundary.
    intersection_expr = (
        f"ST_Intersection(ST_Intersection(u.geometry, s.geometry), {bbox_wkt})"
        if bbox_wkt
        else "ST_Intersection(u.geometry, s.geometry)"
    )
    if bbox:
        log.info("Applying bbox filter: xmin=%s ymin=%s xmax=%s ymax=%s", *bbox)

    if explain:
        log.info("Query plan (with execution metrics):")
        plan = sd.sql(f"""
            EXPLAIN ANALYZE
            SELECT
                u.usrn,
                u.street_type,
                {intersection_expr} AS geometry,
                s."MUSID",
                s."MAP_SYMBOL",
                s."MU_NAME",
                s."DESC_",
                s."GEOLOGY",
                s."DOM_SOILS",
                s."ASSOC_SOIL",
                s."SITE",
                s."CROP_LU",
                s."SOILSCAPE",
                s."DRAINAGE",
                s."FERTILITY",
                s."HABITATS",
                s."DRAINS_TO",
                s."WATER_PROT",
                s."SOILGUIDE"
            FROM usrns AS u
            JOIN soil AS s
              ON ST_Intersects(u.geometry, s.geometry)
            {bbox_filter}
        """)
        plan.show(width=400)

    log.info("Running spatial join...")
    result = sd.sql(f"""
        SELECT
            u.usrn,
            u.street_type,
            {intersection_expr} AS geometry,
            s."MUSID",
            s."MAP_SYMBOL",
            s."MU_NAME",
            s."DESC_",
            s."GEOLOGY",
            s."DOM_SOILS",
            s."ASSOC_SOIL",
            s."SITE",
            s."CROP_LU",
            s."SOILSCAPE",
            s."DRAINAGE",
            s."FERTILITY",
            s."HABITATS",
            s."DRAINS_TO",
            s."WATER_PROT",
            s."SOILGUIDE"
        FROM usrns AS u
        JOIN soil AS s
          ON ST_Intersects(u.geometry, s.geometry)
        {bbox_filter}
        ORDER BY u.usrn
    """)

    log.info("Result row count: %d", result.count())
    return result
