import json
import logging
import pathlib
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .config import DatasetConfig
from .logger import get_logger

log: logging.Logger = get_logger()

# EPSG:27700 (British National Grid) extent used as the Hilbert sort envelope.
# ST_Hilbert maps each geometry's centroid to a Hilbert curve index within this bbox,
# so spatially nearby features get consecutive indices and land in the same row groups.
# Shared by prepare_dataset / prepare_from_csv (file write) and _build_dtf_gdf (in-memory sort).
_BNG_BOX = "{'min_x': 0.0, 'min_y': 0.0, 'max_x': 700000.0, 'max_y': 1300000.0}::BOX_2D"


def _patch_covering_metadata(
    path: pathlib.Path,
    row_group_size: int,
    crs: str | None = None,
) -> None:
    """Patch a GeoParquet file's geo metadata to add the GeoParquet 1.1 covering key.

    DuckDB writes GeoParquet 1.0.0 metadata (no covering key, no CRS) when using
    ``COPY ... TO ... (FORMAT PARQUET)``.  This function:

    - Upgrades ``version`` to ``"1.1.0"``
    - Adds the ``covering`` key pointing at the ``bbox`` struct column
      (which DuckDB already wrote into the file via SQL)
    - Optionally patches the CRS PROJJSON into the geometry column metadata
      (always needed for DuckDB-written files; ``write_geoparquet`` gets CRS from
      geopandas and does not need to pass ``crs`` here)
    - Normalises ``Utf8View`` → ``Utf8`` for downstream compatibility
    - Rewrites the file in-place with ZSTD compression
    """
    table = pq.read_table(str(path))
    geo_meta = json.loads(table.schema.metadata[b"geo"])
    geom_col: str = geo_meta.get("primary_column", "geometry")

    geo_meta["version"] = "1.1.0"
    geo_meta["columns"][geom_col]["covering"] = {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }

    if crs is not None:
        from pyproj import CRS as ProjCRS

        geo_meta["columns"][geom_col]["crs"] = ProjCRS.from_user_input(
            crs
        ).to_json_dict()

    normalised_fields = [
        f.with_type(pa.utf8()) if f.type == pa.string_view() else f
        for f in table.schema
    ]
    schema_meta: dict[bytes, bytes] = {
        **table.schema.metadata,
        b"geo": json.dumps(geo_meta).encode(),
    }
    normalised_schema = pa.schema(normalised_fields, metadata=schema_meta)
    table = table.cast(normalised_schema)

    pq.write_table(table, str(path), row_group_size=row_group_size, compression="zstd")


def _get_src_geometry_col(con: Any, source_path: str) -> str:
    """Return the geometry column name as exposed by DuckDB's ``st_read``.

    The column name in the output of ``st_read`` is determined by the source
    file's internal metadata (e.g. GeoPackage layers typically expose ``"geom"``
    regardless of the original column name).  This helper queries the schema
    so the caller can rename it to ``"geometry"`` in the ``SELECT``.
    """
    rows = con.sql(f"DESCRIBE SELECT * FROM st_read('{source_path}')").fetchall()
    for row in rows:
        col_name: str = row[0]
        col_type: str = row[1]
        if "GEOMETRY" in col_type.upper():
            return col_name
    raise ValueError(f"No GEOMETRY column found in {source_path!r}")


def prepare_dataset(config: DatasetConfig, force: bool = False) -> pathlib.Path:
    """Read any spatial dataset and write an optimised GeoParquet 1.1 file.

    The output is spatially sorted by Hilbert curve index, ZSTD-compressed, and
    includes GeoParquet 1.1 bbox covering columns so SedonaDB can prune row groups
    during spatial joins.

    Uses DuckDB's ``ST_Hilbert()`` function to compute a Z-order (Hilbert) index
    from each geometry's centroid within the EPSG:27700 (British National Grid)
    extent.  Sorting by this index clusters spatially adjacent features into
    consecutive row groups, maximising SedonaDB's ability to skip irrelevant row
    groups when a bbox filter is applied.

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
    if not force and config.parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            config.parquet_path,
        )
        return config.parquet_path

    config.parquet_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("Preparing %s from %s", config.name, config.source_path)

    # CRS validation — lightweight check before starting the DuckDB pipeline
    import pyogrio

    info = pyogrio.read_info(str(config.source_path))
    assert info["crs"] == config.crs, (
        f"Expected CRS {config.crs}, got {info['crs']} for {config.source_path}"
    )
    feature_count: int = info.get("features", -1)
    log.info(
        "  CRS: %s | features: %s | geometry: %s",
        info["crs"],
        f"{feature_count:,}" if feature_count >= 0 else "unknown",
        info.get("geometry_type", "unknown"),
    )

    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Discover the geometry column name from the source file.
    # DuckDB's st_read exposes the column using the file's internal name
    # (e.g. GeoPackage layers typically use "geom"), which may differ from
    # config.geometry_column.  We rename it to "geometry" in the SELECT.
    src_geom: str = _get_src_geometry_col(con, str(config.source_path))
    log.info("  Source geometry column: %r → output column: 'geometry'", src_geom)
    log.info("  Hilbert sort + write → %s ...", config.parquet_path)

    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                * EXCLUDE "{src_geom}",
                "{src_geom}" AS geometry,
                {{
                    'xmin': ST_XMin("{src_geom}"),
                    'ymin': ST_YMin("{src_geom}"),
                    'xmax': ST_XMax("{src_geom}"),
                    'ymax': ST_YMax("{src_geom}")
                }} AS bbox
            FROM st_read('{config.source_path}')
            ORDER BY ST_Hilbert("{src_geom}", {_BNG_BOX})
        ) TO '{config.parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {config.row_group_size})
    """)
    _patch_covering_metadata(config.parquet_path, config.row_group_size, crs=config.crs)
    elapsed = time.perf_counter() - t0

    pq_meta = pq.read_metadata(str(config.parquet_path))
    file_mb = config.parquet_path.stat().st_size / 1024 / 1024
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_meta.num_rows:,}",
        pq_meta.num_row_groups,
        file_mb,
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

    Follows the same Hilbert-sort + bbox-covering + ZSTD pipeline as
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
    resolved_parquet: pathlib.Path = pathlib.Path(parquet_path)

    if not force and resolved_parquet.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            resolved_parquet,
        )
        return resolved_parquet

    resolved_parquet.parent.mkdir(parents=True, exist_ok=True)
    resolved_csv: pathlib.Path = pathlib.Path(csv_path)

    log.info("Preparing CSV → GeoParquet: %s", resolved_csv)

    match geometry_type:
        case "point":
            log.info("  Geometry: point from (%r, %r) | CRS: %s", x_col, y_col, crs)
        case _:
            raise NotImplementedError(
                f"geometry_type={geometry_type!r} is not yet supported. "
                "Currently only 'point' is implemented."
            )

    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Count rows first so we can report it before the (potentially slow) sort+write.
    _count_row = con.sql(
        f"SELECT COUNT(*) FROM read_csv('{resolved_csv}', auto_detect=true)"
    ).fetchone()
    row_count: int = _count_row[0] if _count_row else 0
    log.info(
        "  Rows: %s | Hilbert sort + write → %s ...", f"{row_count:,}", resolved_parquet
    )

    # Build point geometries from X/Y columns, sort by Hilbert index, add bbox struct.
    # A subquery materialises the geometry column so ST_Hilbert and the bbox struct
    # can reference it by name without repeating the ST_Point(...) expression.
    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                *,
                {{
                    'xmin': ST_XMin(geometry),
                    'ymin': ST_YMin(geometry),
                    'xmax': ST_XMax(geometry),
                    'ymax': ST_YMax(geometry)
                }} AS bbox
            FROM (
                SELECT
                    * EXCLUDE ("{x_col}", "{y_col}"),
                    ST_Point("{x_col}", "{y_col}") AS geometry
                FROM read_csv('{resolved_csv}', auto_detect=true, null_padding=true)
            )
            ORDER BY ST_Hilbert(geometry, {_BNG_BOX})
        ) TO '{resolved_parquet}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size})
    """)
    _patch_covering_metadata(resolved_parquet, row_group_size, crs=crs)
    elapsed = time.perf_counter() - t0

    pq_meta = pq.read_metadata(str(resolved_parquet))
    file_mb = resolved_parquet.stat().st_size / 1024 / 1024
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_meta.num_rows:,}",
        pq_meta.num_row_groups,
        file_mb,
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
