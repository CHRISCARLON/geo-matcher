import json
import logging
import pathlib
from typing import Any

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from .config import DatasetConfig
from .logger import get_logger

log: logging.Logger = get_logger()


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
    tmp: pathlib.Path = path.with_suffix(".tmp.parquet")

    gdf.to_parquet(str(tmp), row_group_size=row_group_size, index=False)
    base: pa.Table = pq.read_table(str(tmp))
    geo_meta: dict[str, Any] = json.loads(base.schema.metadata[b"geo"])

    log.info(f"The geometry metadat for this dataset is: {geo_meta}")

    tmp.unlink()

    geom_col: str = geo_meta.get("primary_column", "geometry")

    # Build bbox struct column from shapely bounds
    # TODO: Figure out a way to use the rust writer here
    # and not manually patch it in.
    # The rust GeoParquetWriter doesn't let you
    # specify row_group numbers?
    # HACK: Best option for now?
    bounds: pd.DataFrame = gdf.geometry.bounds
    bbox_col: pa.StructArray = pa.StructArray.from_arrays(
        [
            pa.array(bounds["minx"].to_numpy(), type=pa.float64()),
            pa.array(bounds["miny"].to_numpy(), type=pa.float64()),
            pa.array(bounds["maxx"].to_numpy(), type=pa.float64()),
            pa.array(bounds["maxy"].to_numpy(), type=pa.float64()),
        ],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    table: pa.Table = base.append_column("bbox", bbox_col)

    # Patch geo metadata: version 1.1.0 + covering key
    # This is so Sedona can use this when a bbox is supplied for a join
    # HACK: Shouldn't really have to manually do this?
    geo_meta["version"] = "1.1.0"
    geo_meta["columns"][geom_col]["covering"] = {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }
    schema_meta: dict[bytes, bytes] = {
        **table.schema.metadata,
        b"geo": json.dumps(geo_meta).encode(),
    }

    log.info(f"The schema metadata is: {schema_meta}")

    # PyArrow writes string columns as Utf8View by default
    # TODO: Think I got confused and I don't need this bit
    normalised_fields = [
        f.with_type(pa.utf8()) if f.type == pa.string_view() else f
        for f in table.schema
    ]
    normalised_schema = pa.schema(normalised_fields, metadata=schema_meta)

    log.info(f"The normlaised schema  metadata is: {normalised_schema}")

    table = table.cast(normalised_schema)

    # TODO: is there not a native python Geoparquet writer?
    pq.write_table(
        table,
        str(path),
        row_group_size=row_group_size,
        compression="zstd",
    )


def prepare_dataset(config: DatasetConfig, force: bool = False) -> pathlib.Path:
    """Read any spatial dataset and write an optimised GeoParquet 1.1 file.

    The output is spatially sorted, ZSTD-compressed, and includes GeoParquet 1.1
    bbox covering columns so SedonaDB can prune row groups during spatial joins.

    Parameters
    ----------
    config:
        Dataset configuration describing the source file, output path, geometry
        column name, row group size, and expected CRS.
    force:
        If ``True``, re-prepare even if ``config.parquet_path`` already exists.
        If ``False`` (default), skip preparation when the file is present.

    Returns
    -------
    pathlib.Path
        Path to the written (or already-existing) GeoParquet file.
    """

    # TODO: shift this over to DuckDB and not don't use GeoPandas

    if not force and config.parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            config.parquet_path,
        )
        return config.parquet_path

    config.parquet_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("Reading %s from %s ...", config.name, config.source_path)

    gdf: gpd.GeoDataFrame = gpd.read_file(str(config.source_path), engine="pyogrio")

    log.info("  CRS: %s, rows: %d, columns: %s", gdf.crs, len(gdf), list(gdf.columns))

    assert str(gdf.crs) == config.crs, (
        f"Expected CRS {config.crs}, got {gdf.crs} for {config.source_path}"
    )

    if gdf.geometry.name != "geometry":
        gdf = gdf.rename_geometry("geometry")

    gdf = gpd.GeoDataFrame(gdf.sort_values("geometry").reset_index(drop=True))

    _write_geoparquet(gdf, config.parquet_path, config.row_group_size)

    log.info(
        "  Written %s (GeoParquet 1.1 / WKB + covering, row_group_size=%d)",
        config.parquet_path,
        config.row_group_size,
    )
    return config.parquet_path


def prepare_from_csv(
    csv_path: str | pathlib.Path,
    parquet_path: str | pathlib.Path,
    geometry_type: str = "point",
    x_col: str = "Easting",
    y_col: str = "Northing",
    crs: str = "EPSG:27700",
    row_group_size: int = 10_000,
    force: bool = False,
) -> pathlib.Path:
    """Read a CSV and write an optimised GeoParquet 1.1 file.

    Follows the same spatial-sort + bbox-covering + ZSTD pipeline as
    :func:`prepare_dataset`.

    Parameters
    ----------
    csv_path:
        Path to the source CSV file.
    parquet_path:
        Destination path for the GeoParquet output.
    geometry_type:
        How to build geometries from the CSV. Currently supports ``"point"``
        (default) — builds point geometries from ``x_col`` / ``y_col``.
    x_col:
        Column holding the X / Easting coordinate (``"point"`` only).
    y_col:
        Column holding the Y / Northing coordinate (``"point"`` only).
    crs:
        Coordinate reference system of the coordinate columns (default EPSG:27700).
    row_group_size:
        Row group size for the output GeoParquet.
    force:
        Re-prepare even if the output file already exists.

    Returns
    -------
    pathlib.Path
        Path to the written (or already-existing) GeoParquet file.
    """
    # TODO: Move this into a single dispatch function with
    # the main prepare dataset function

    # TODO: Move away from Pandas
    resolved_parquet: pathlib.Path = pathlib.Path(parquet_path)

    if not force and resolved_parquet.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            resolved_parquet,
        )
        return resolved_parquet

    resolved_parquet.parent.mkdir(parents=True, exist_ok=True)
    resolved_csv: pathlib.Path = pathlib.Path(csv_path)

    log.info("Reading CSV from %s ...", resolved_csv)

    df: pd.DataFrame = pd.read_csv(str(resolved_csv), low_memory=False)
    obj_cols: list[str] = list(df.select_dtypes(include="object").columns)
    df[obj_cols] = df[obj_cols].where(df[obj_cols].notna(), other=None)

    log.info("  rows: %d, columns: %s", len(df), list(df.columns))

    match geometry_type:
        case "point":
            keep_cols: list[str] = [c for c in df.columns if c not in (x_col, y_col)]
            geoms = [Point(x, y) for x, y in zip(df[x_col], df[y_col])]
            gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
                df[keep_cols], geometry=geoms, crs=crs
            )
        case _:
            raise NotImplementedError(
                f"geometry_type={geometry_type!r} is not yet supported. "
                "Currently only 'point' is implemented."
            )

    gdf = gpd.GeoDataFrame(gdf.sort_values("geometry").reset_index(drop=True))
    _write_geoparquet(gdf, resolved_parquet, row_group_size)
    log.info(
        "  Written %s (GeoParquet 1.1 / WKB + covering, row_group_size=%d)",
        resolved_parquet,
        row_group_size,
    )
    return resolved_parquet


def prepare_usrns(
    usrn_gpkg: str | pathlib.Path,
    parquet_path: str | pathlib.Path,
    force: bool = False,
) -> pathlib.Path:
    """Prepare the open USRNs GeoPackage as an optimised GeoParquet 1.1 file.

    Convenience wrapper around :func:`prepare_dataset` pre-configured for the
    OS Open USRN dataset (line geometries, ``row_group_size=20_000``).

    Parameters
    ----------
    usrn_gpkg:
        Path to the OS Open USRN GeoPackage.
    parquet_path:
        Destination path for the GeoParquet output.
    force:
        Re-prepare even if the output file already exists.

    Returns
    -------
    pathlib.Path
        Path to the written GeoParquet file.
    """
    config: DatasetConfig = DatasetConfig(
        name="usrns",
        source_path=pathlib.Path(usrn_gpkg),
        parquet_path=pathlib.Path(parquet_path),
        columns=[],
        geometry_column="geometry",
        row_group_size=20_000,
    )
    return prepare_dataset(config, force=force)
