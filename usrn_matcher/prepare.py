import json
import logging
import pathlib
import time
from typing import Any, TypedDict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from pyproj import CRS as ProjCRS

from .config import CsvSource, DatasetConfig, OgrSource, ParquetSource, UsrnLineSource
from .logger import get_logger

log: logging.Logger = get_logger()

# EPSG:27700 (British National Grid) extent used as the Hilbert sort envelope.
# ST_Hilbert maps each geometry's centroid to a Hilbert curve index within this bbox,
# so spatially nearby features get consecutive indices and land in the same row groups.
# Shared by prepare_dataset / prepare_from_csv (file write) and _build_dtf_table (in-memory sort).
_BNG_BOX = "{'min_x': 0.0, 'min_y': 0.0, 'max_x': 700000.0, 'max_y': 1300000.0}::BOX_2D"


class _CoveringColumn(TypedDict):
    xmin: list[str]
    ymin: list[str]
    xmax: list[str]
    ymax: list[str]


class _Covering(TypedDict):
    bbox: _CoveringColumn


class _GeomColumnMeta(TypedDict, total=False):
    encoding: str
    geometry_types: list[str]
    crs: dict[str, Any]
    covering: _Covering


class _GeoMeta(TypedDict):
    version: str
    primary_column: str
    columns: dict[str, _GeomColumnMeta]


_EXPECTED_COVERING_METADATA: _Covering = {
    "bbox": {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }
}


def _patch_covering_metadata(
    path: pathlib.Path,
    row_group_size: int,
    crs: str | None = None,
    primary_column: str | None = None,
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
    geo_meta: _GeoMeta = json.loads(table.schema.metadata[b"geo"])
    if primary_column is not None:
        geo_meta["primary_column"] = primary_column
    geom_col: str = geo_meta.get("primary_column", "geometry")

    geo_meta["version"] = "1.1.0"
    geo_meta["columns"][geom_col]["covering"] = _EXPECTED_COVERING_METADATA

    if crs is not None:
        geo_meta["columns"][geom_col]["crs"] = ProjCRS.from_user_input(
            crs
        ).to_json_dict()

    normalised_fields: list[Any] = [
        field.with_type(pa.utf8()) if field.type == pa.string_view() else field
        for field in table.schema
    ]
    schema_meta: dict[bytes, bytes] = {
        **table.schema.metadata,
        b"geo": json.dumps(geo_meta).encode(),
    }
    normalised_schema = pa.schema(normalised_fields, metadata=schema_meta)
    table = table.cast(normalised_schema)

    pq.write_table(table, str(path), row_group_size=row_group_size, compression="zstd")
    if geo_meta["columns"][geom_col].get("covering") != _EXPECTED_COVERING_METADATA:
        raise RuntimeError(
            f"Failed to patch GeoParquet covering metadata for {geom_col!r} in {path}"
        )


def _get_src_geometry_col(con: Any, source_path: str) -> str:
    """Return the geometry column name as exposed by DuckDB's ``st_read``.

    This looks directly for "GEOMETRY".

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


def _prepare_ogr(
    source: OgrSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if not force and parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            parquet_path,
        )
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Preparing %s from %s", name, source.path)

    info = pyogrio.read_info(str(source.path))
    assert info["crs"] == source.crs, (
        f"Expected CRS {source.crs}, got {info['crs']} for {source.path}"
    )
    feature_count: int = info.get("features", -1)
    log.info(
        "  CRS: %s | features: %s | geometry: %s",
        info["crs"],
        f"{feature_count:,}" if feature_count >= 0 else "unknown",
        info.get("geometry_type", "unknown"),
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if threads is not None:
        con.execute(f"SET threads = {threads};")

    src_geom: str = _get_src_geometry_col(con, str(source.path))
    log.info("  Source geometry column: %r → output column: 'geometry'", src_geom)
    log.info("  Hilbert sort + write → %s ...", parquet_path)

    t0 = time.perf_counter()
    # SPATIALLY SORT THE GEOPARQUET FILE
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
            FROM st_read('{source.path}')
            ORDER BY ST_Hilbert("{src_geom}", {_BNG_BOX})
        ) TO '{parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {source.row_group_size})
    """)

    # NEED TO RE-READ IN THE PARQUET FILE TO PATCH THE COVERING DATA
    _patch_covering_metadata(parquet_path, source.row_group_size, crs=source.crs)
    elapsed = time.perf_counter() - t0

    pq_meta = pq.read_metadata(str(parquet_path))
    file_mb = parquet_path.stat().st_size / 1024 / 1024
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_meta.num_rows:,}",
        pq_meta.num_row_groups,
        file_mb,
    )
    return parquet_path


def _prepare_csv(
    source: CsvSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if not force and parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            parquet_path,
        )
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Preparing CSV → GeoParquet: %s", source.path)

    match source.geometry_type:
        case "point":
            log.info(
                "  Geometry: point from (%r, %r) | CRS: %s",
                source.x_col,
                source.y_col,
                source.crs,
            )
        case _:
            raise NotImplementedError(
                f"geometry_type={source.geometry_type!r} is not yet supported. "
                "Currently only 'point' is implemented."
            )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if threads is not None:
        con.execute(f"SET threads = {threads};")

    _count_row = con.sql(
        f"SELECT COUNT(*) FROM read_csv('{source.path}', auto_detect=true, nullstr=['NULL', ''])"
    ).fetchone()
    row_count: int = _count_row[0] if _count_row else 0
    log.info(
        "  Rows: %s | Hilbert sort + write → %s ...", f"{row_count:,}", parquet_path
    )

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
                    * EXCLUDE ("{source.x_col}", "{source.y_col}"),
                    ST_Point("{source.x_col}", "{source.y_col}") AS geometry
                FROM read_csv('{source.path}', auto_detect=true, null_padding=true, nullstr=['NULL', ''])
            )
            ORDER BY ST_Hilbert(geometry, {_BNG_BOX})
        ) TO '{parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {source.row_group_size})
    """)
    _patch_covering_metadata(parquet_path, source.row_group_size, crs=source.crs)
    elapsed = time.perf_counter() - t0

    pq_meta = pq.read_metadata(str(parquet_path))
    file_mb = parquet_path.stat().st_size / 1024 / 1024
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_meta.num_rows:,}",
        pq_meta.num_row_groups,
        file_mb,
    )
    return parquet_path


def _prepare_parquet(
    source: ParquetSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if not force and parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            parquet_path,
        )
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Preparing %s from %s", name, source.path)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if threads is not None:
        con.execute(f"SET threads = {threads};")

    pq_src_meta = pq.read_metadata(str(source.path))
    log.info(
        "  Source: %s rows | %d row groups | Hilbert sort + write → %s ...",
        f"{pq_src_meta.num_rows:,}",
        pq_src_meta.num_row_groups,
        parquet_path,
    )

    geom_col = source.geometry_col
    if source.source_crs is not None:
        # Native GEOMETRY column in a foreign CRS — reproject to target CRS.
        # always_xy=true forces lon/lat (x/y) axis order, overriding PROJ 6+'s
        # official axis order for EPSG:4326 (which is lat/lon). Without this,
        # DuckDB interprets the first coordinate as latitude, swapping axes and
        # producing completely wrong output coordinates.
        geom_expr = f"ST_Transform({geom_col}, '{source.source_crs}', '{source.crs}', always_xy := true)"
    elif geom_col == "geometry":
        # WKB blob written by this pipeline — must promote to GEOMETRY explicitly.
        geom_expr = "ST_GeomFromWKB(geometry)"
    else:
        # Native GEOMETRY column already in the target CRS (no reprojection needed).
        geom_expr = geom_col

    # Build the EXCLUDE list for the inner SELECT.
    # For pipeline files (WKB geometry column) also drop the existing bbox so the
    # outer query can rebuild it against the recomputed geometry.
    # For external files, detect and drop all other geometry columns from the source
    # so DuckDB only sees one geometry column in the output — otherwise it may pick
    # a different column as the GeoParquet primary column and _patch_covering_metadata
    # will patch the wrong entry, leaving our geometry with the wrong CRS in metadata.
    if geom_col == "geometry" and source.source_crs is None:
        exclude_cols = "geometry, bbox"
    else:
        src_describe = con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{source.path}')"
        ).fetchall()
        src_col_names = {row[0] for row in src_describe}
        extra_geom_cols = [
            row[0]
            for row in src_describe
            if "GEOMETRY" in row[1].upper() and row[0] != geom_col
        ]
        cols_to_exclude = [geom_col] + extra_geom_cols
        if "bbox" in src_col_names:
            cols_to_exclude.append("bbox")
        exclude_cols = ", ".join(f'"{c}"' for c in cols_to_exclude)
        if extra_geom_cols:
            log.info("  Excluding extra geometry columns: %s", extra_geom_cols)

    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                * EXCLUDE geometry,
                geometry,
                {{
                    'xmin': ST_XMin(geometry),
                    'ymin': ST_YMin(geometry),
                    'xmax': ST_XMax(geometry),
                    'ymax': ST_YMax(geometry)
                }} AS bbox
            FROM (
                SELECT
                    * EXCLUDE ({exclude_cols}),
                    {geom_expr} AS geometry
                FROM read_parquet('{source.path}')
            )
            ORDER BY ST_Hilbert(geometry, {_BNG_BOX})
        ) TO '{parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {source.row_group_size})
    """)
    _patch_covering_metadata(parquet_path, source.row_group_size, crs=source.crs)
    elapsed = time.perf_counter() - t0

    pq_meta = pq.read_metadata(str(parquet_path))
    file_mb = parquet_path.stat().st_size / 1024 / 1024
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_meta.num_rows:,}",
        pq_meta.num_row_groups,
        file_mb,
    )
    return parquet_path


def _prepare_usrn_line(
    source: UsrnLineSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if not force and parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            parquet_path,
        )
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if threads is not None:
        con.execute(f"SET threads = {threads};")

    pq_src = pq.read_metadata(str(source.path))
    log.info(
        "Preparing %s (buffer=%.0fm) from %s (%s rows / %d row groups)",
        name,
        source.buffer_m,
        source.path,
        f"{pq_src.num_rows:,}",
        pq_src.num_row_groups,
    )

    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            SELECT
                usrn,
                street_type,
                geometry AS geometry_line,
                ST_Buffer(geometry, {source.buffer_m}) AS geometry,
                {{
                    'xmin': ST_XMin(ST_Buffer(geometry, {source.buffer_m})),
                    'ymin': ST_YMin(ST_Buffer(geometry, {source.buffer_m})),
                    'xmax': ST_XMax(ST_Buffer(geometry, {source.buffer_m})),
                    'ymax': ST_YMax(ST_Buffer(geometry, {source.buffer_m}))
                }} AS bbox
            FROM read_parquet('{source.path}')
            ORDER BY ST_Hilbert(geometry, {_BNG_BOX})
        ) TO '{parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {source.row_group_size})
    """)
    _patch_covering_metadata(
        parquet_path, source.row_group_size, crs="EPSG:27700", primary_column="geometry"
    )
    elapsed = time.perf_counter() - t0

    pq_out = pq.read_metadata(str(parquet_path))
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_out.num_rows:,}",
        pq_out.num_row_groups,
        parquet_path.stat().st_size / 1024 / 1024,
    )
    return parquet_path


def prepare(
    config: DatasetConfig,
    force: bool = False,
    threads: int | None = None,
) -> pathlib.Path:
    """Read a spatial source and write an optimised GeoParquet 1.1 file.

    Dispatches to the correct reader based on ``config.source`` type:

    - ``OgrSource`` — any GDAL-readable vector format (GeoPackage, Shapefile, …)
    - ``CsvSource`` — CSV with explicit x/y coordinate columns
    - ``ParquetSource`` — existing GeoParquet to re-sort and re-compress
    - ``UsrnLineSource`` — buffer USRN centrelines for line join Phase 2

    A ``config.source`` must always be provided.

    The output is always Hilbert-sorted, ZSTD-compressed GeoParquet 1.1 with
    bbox covering columns for SedonaDB row-group pruning.

    Parameters
    ----------
    config:
        Dataset configuration. The ``source`` field drives dispatch; all other
        fields describe the output (name, parquet_path, columns).
    force:
        If ``True``, re-prepare even if ``config.parquet_path`` already exists.
    threads:
        Number of DuckDB threads to use. ``None`` lets DuckDB use all available
        cores (default). Set to a lower value to reduce CPU pressure.

    Returns
    -------
    pathlib.Path
        Path to the written (or already-existing) GeoParquet file.
    """
    if config.source is None:
        raise ValueError(
            "DatasetConfig.source must be set. "
            "Use DatasetConfig(source=OgrSource(...)), CsvSource(...), or ParquetSource(...)."
        )
    match config.source:
        case OgrSource() as src:
            return _prepare_ogr(src, config.parquet_path, config.name, force, threads)
        case CsvSource() as src:
            return _prepare_csv(src, config.parquet_path, config.name, force, threads)
        case ParquetSource() as src:
            return _prepare_parquet(
                src, config.parquet_path, config.name, force, threads
            )
        case UsrnLineSource() as src:
            return _prepare_usrn_line(
                src, config.parquet_path, config.name, force, threads
            )
