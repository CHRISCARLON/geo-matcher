import json
import pathlib

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

from .logger import get_logger

log = get_logger()


def _write_geoparquet(
    gdf: gpd.GeoDataFrame, path: pathlib.Path, row_group_size: int
) -> None:
    """Write a GeoDataFrame as GeoParquet 1.1 with bbox covering columns.

    Builds the Arrow table directly from the in-memory GeoDataFrame:
      - geometry encoded as WKB
      - per-row bbox struct column computed from shapely bounds
      - geo metadata patched to GeoParquet 1.1 with covering key

    Uses pq.write_table so row_group_size is respected.

    Aim is to give SedonaDB the fine-grained row groups it needs for spatial pruning.
    """
    # Write temp file to get geopandas geo metadata (CRS PROJJSON, geometry_types, bbox)
    tmp = path.with_suffix(".tmp.parquet")
    gdf.to_parquet(str(tmp), row_group_size=row_group_size, index=False)
    base = pq.read_table(str(tmp))
    geo_meta = json.loads(base.schema.metadata[b"geo"])
    tmp.unlink()

    geom_col = geo_meta.get("primary_column", "geometry")

    # Build bbox struct column from shapely bounds
    # TODO: Figure out a way to use the rust writer here
    # and not manually patch it in
    bounds = gdf.geometry.bounds
    bbox_col = pa.StructArray.from_arrays(
        [
            pa.array(bounds["minx"].to_numpy(), type=pa.float64()),
            pa.array(bounds["miny"].to_numpy(), type=pa.float64()),
            pa.array(bounds["maxx"].to_numpy(), type=pa.float64()),
            pa.array(bounds["maxy"].to_numpy(), type=pa.float64()),
        ],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    table = base.append_column("bbox", bbox_col)

    # Patch geo metadata: version 1.1.0 + covering key
    # This is so Sedona can use this when a bbox is supplied for a join
    geo_meta["version"] = "1.1.0"
    geo_meta["columns"][geom_col]["covering"] = {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }
    schema_meta = {**table.schema.metadata, b"geo": json.dumps(geo_meta).encode()}
    table = table.cast(table.schema.with_metadata(schema_meta))

    pq.write_table(
        table,
        str(path),
        row_group_size=row_group_size,
        compression="zstd",
    )


def convert_gpkgs(
    usrn_gpkg: pathlib.Path,
    soil_gpkg: pathlib.Path,
    usrn_parquet: pathlib.Path,
    soil_parquet: pathlib.Path,
) -> None:
    """Convert USRN and soil GeoPackages to optimised GeoParquet 1.1 files."""
    # Convert soil data
    log.info("Reading soil GPKG...")
    soil = gpd.read_file(str(soil_gpkg), engine="pyogrio")
    log.info(
        "  CRS: %s, rows: %d, columns: %s", soil.crs, len(soil), list(soil.columns)
    )
    assert str(soil.crs) == "EPSG:27700", f"Expected EPSG:27700, got {soil.crs}"

    if "SHAPE" in soil.columns and "geometry" not in soil.columns:
        soil = soil.rename_geometry("geometry")

    soil = gpd.GeoDataFrame(soil.sort_values("geometry").reset_index(drop=True))
    _write_geoparquet(soil, soil_parquet, row_group_size=10_000)
    log.info("  Written %s (GeoParquet 1.1 / WKB + covering)", soil_parquet)

    # Convert USRN data
    log.info("Reading USRN GPKG...")
    usrns = gpd.read_file(str(usrn_gpkg), engine="pyogrio")
    log.info(
        "  CRS: %s, rows: %d, columns: %s", usrns.crs, len(usrns), list(usrns.columns)
    )
    assert str(usrns.crs) == "EPSG:27700", f"Expected EPSG:27700, got {usrns.crs}"

    usrns = gpd.GeoDataFrame(usrns.sort_values("geometry").reset_index(drop=True))
    _write_geoparquet(usrns, usrn_parquet, row_group_size=20_000)
    log.info("  Written %s (GeoParquet 1.1 / WKB + covering)", usrn_parquet)
