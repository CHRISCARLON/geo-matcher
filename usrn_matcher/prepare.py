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

from .config import (
    CsvSource,
    DatasetConfig,
    GeometryType,
    OgrSource,
    ParquetSource,
    UsrnSource,
)
from .logger import get_logger

log: logging.Logger = get_logger()

# EPSG:27700 (British National Grid) extent used as the Hilbert sort envelope.
# ST_Hilbert maps each geometry's centroid to a Hilbert curve index within this bbox,
# so spatially nearby features get consecutive indices and land in the same row groups.
# Shared by every _prepare_* writer (via _write_geoparquet) as the ORDER BY envelope.
_BNG_BOX = "{'min_x': 0.0, 'min_y': 0.0, 'max_x': 700000.0, 'max_y': 1300000.0}::BOX_2D"


def _sql_str(value: object) -> str:
    """Escape a value for safe interpolation inside a single-quoted SQL string literal.

    DuckDB paths/CRS strings are inlined into SQL (no parameter binding in ``COPY``
    statements), so a value containing ``'`` would otherwise break or inject the query.
    """
    return str(value).replace("'", "''")


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
      (always needed for DuckDB-written files; ``write_geoparquet``.
    - Normalises ``Utf8View`` → ``Utf8`` for downstream compatibility
    - Rewrites the file in-place with ZSTD compression
    """
    # Read the data in here
    table = pq.read_table(str(path))

    # Create the geo metadata structure
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


def _get_src_geometry_col(con: duckdb.DuckDBPyConnection, source_path: str) -> str:
    """Return the geometry column name as exposed by DuckDB's ``st_read``.

    This looks directly for "GEOMETRY".

    The column name in the output of ``st_read`` is determined by the source
    file's internal metadata (e.g. GeoPackage layers typically expose ``"geom"``
    regardless of the original column name).  This helper queries the schema
    so the caller can rename it to ``"geometry"`` in the ``SELECT``.
    """
    rows = con.sql(
        f"DESCRIBE SELECT * FROM st_read('{_sql_str(source_path)}')"
    ).fetchall()
    for row in rows:
        col_name: str = row[0]
        col_type: str = row[1]
        if "GEOMETRY" in col_type.upper():
            return col_name
    found = ", ".join(f"{row[0]} ({row[1]})" for row in rows)
    raise ValueError(
        f"No GEOMETRY column found in {source_path!r}. Columns found: {found}"
    )


# ---------------------------------------------------------------------------
# Shared writer helpers
#
# Every _prepare_* function below funnels through the same shape: skip if the
# output already exists, open a DuckDB connection, build a *core* SELECT that
# is unique to that source type (must materialise a `geometry` column), then
# hand it to _write_geoparquet to add the bbox covering struct, Hilbert-sort,
# write the GeoParquet file, and patch its metadata.
# ---------------------------------------------------------------------------


def _should_skip(parquet_path: pathlib.Path, force: bool) -> bool:
    """Return True (after logging) if the output already exists and force=False."""
    if not force and parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            parquet_path,
        )
        return True
    return False


def _open_connection(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the spatial extension loaded."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if threads is not None:
        con.execute(f"SET threads = {threads};")
    return con


def _bbox_struct_sql(geom_col: str = "geometry") -> str:
    """Return the ``{'xmin': ..., ...} AS bbox``-ready struct expression for ``geom_col``."""
    return (
        "{"
        f"'xmin': ST_XMin({geom_col}), 'ymin': ST_YMin({geom_col}), "
        f"'xmax': ST_XMax({geom_col}), 'ymax': ST_YMax({geom_col})"
        "}"
    )


def _write_geoparquet(
    con: duckdb.DuckDBPyConnection,
    core_select_sql: str,
    parquet_path: pathlib.Path,
    row_group_size: int,
    crs: str | None = None,
    primary_column: str | None = None,
) -> float:
    """Wrap ``core_select_sql`` with the bbox covering struct + Hilbert sort, write it
    out as GeoParquet, and patch the covering/CRS metadata.

    ``core_select_sql`` must be a full ``SELECT ...`` statement that already produces
    a materialised ``geometry`` column — this function only adds the bbox struct
    (computed once, against that materialised column) and the ``ORDER BY
    ST_Hilbert(...)`` envelope around it.
    """
    copy_sql = f"""
        COPY (
            SELECT
                *,
                {_bbox_struct_sql()} AS bbox
            FROM (
                {core_select_sql}
            )
            ORDER BY ST_Hilbert(geometry, {_BNG_BOX})
        ) TO '{_sql_str(parquet_path)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size})
    """
    log.debug("  COPY SQL:%s", copy_sql)
    t0 = time.perf_counter()
    con.execute(copy_sql)
    _patch_covering_metadata(
        parquet_path, row_group_size, crs=crs, primary_column=primary_column
    )
    return time.perf_counter() - t0


def _log_prepared(parquet_path: pathlib.Path, elapsed: float) -> None:
    """Log the standard "Done in Xs — N rows | M row groups | Y MB" summary line."""
    pq_meta = pq.read_metadata(str(parquet_path))
    file_mb = parquet_path.stat().st_size / 1024 / 1024
    log.info(
        "  Done in %.1fs — %s rows | %d row groups | %.1f MB",
        elapsed,
        f"{pq_meta.num_rows:,}",
        pq_meta.num_row_groups,
        file_mb,
    )


def _prepare_ogr(
    source: OgrSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if _should_skip(parquet_path, force):
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Preparing %s from %s", name, source.path)

    info = pyogrio.read_info(str(source.path))
    if info["crs"] != source.crs:
        raise ValueError(
            f"Expected CRS {source.crs}, got {info['crs']} for {source.path}"
        )
    feature_count: int = info.get("features", -1)
    log.info(
        "  CRS: %s | features: %s | geometry: %s",
        info["crs"],
        f"{feature_count:,}" if feature_count >= 0 else "unknown",
        info.get("geometry_type", "unknown"),
    )

    con = _open_connection(threads)

    src_geom: str = _get_src_geometry_col(con, str(source.path))
    log.info("  Source geometry column: %r → output column: 'geometry'", src_geom)
    log.info("  Hilbert sort + write → %s ...", parquet_path)

    core_select_sql = f"""
        SELECT
            * EXCLUDE "{src_geom}",
            "{src_geom}" AS geometry
        FROM st_read('{_sql_str(source.path)}')
    """
    elapsed = _write_geoparquet(
        con, core_select_sql, parquet_path, source.row_group_size, crs=source.crs
    )
    _log_prepared(parquet_path, elapsed)
    return parquet_path


def _csv_geometry_sql(source: CsvSource) -> tuple[str, str]:
    """Return ``(geometry_expr_sql, exclude_cols_sql)`` for building a CsvSource's geometry.

    ``geometry_expr_sql`` is a DuckDB expression producing a GEOMETRY value;
    ``exclude_cols_sql`` names the raw source column(s) consumed to build it, ready to
    drop from the output via ``* EXCLUDE (...)``.
    """
    # CsvSource.__post_init__ already normalises geometry_type to a real GeometryType
    # member at construction time; re-wrapping here just re-narrows the static type
    # (the field itself is typed GeometryType | str to accept plain strings from the
    # CLI).
    geometry_type: GeometryType = GeometryType(source.geometry_type)
    match geometry_type:
        case GeometryType.POINT:
            return (
                f'ST_Point("{source.x_col}", "{source.y_col}")',
                f'"{source.x_col}", "{source.y_col}"',
            )
        case GeometryType.LINE | GeometryType.POLYGON:
            # CsvSource.__post_init__ guarantees wkt_col is set whenever
            # geometry_type is LINE/POLYGON.
            assert source.wkt_col is not None
            return f'ST_GeomFromText("{source.wkt_col}")', f'"{source.wkt_col}"'


def _prepare_csv(
    source: CsvSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if _should_skip(parquet_path, force):
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Preparing CSV → GeoParquet: %s", source.path)

    geom_sql, exclude_sql = _csv_geometry_sql(source)
    log.info(
        "  Source geometry: %s → output column: 'geometry' | CRS: %s",
        geom_sql,
        source.crs,
    )

    con = _open_connection(threads)

    _count_row = con.sql(
        f"SELECT COUNT(*) FROM read_csv('{_sql_str(source.path)}', auto_detect=true, nullstr=['NULL', ''])"
    ).fetchone()
    row_count: int = _count_row[0] if _count_row else 0
    log.info("  Rows: %s", f"{row_count:,}")
    log.info("  Hilbert sort + write → %s ...", parquet_path)

    # A subquery materialises the geometry column so the outer bbox struct and
    # ORDER BY can reference it by name without repeating the geometry expression.
    core_select_sql = f"""
        SELECT
            * EXCLUDE ({exclude_sql}),
            {geom_sql} AS geometry
        FROM read_csv('{_sql_str(source.path)}', auto_detect=true, null_padding=true, nullstr=['NULL', ''])
    """
    elapsed = _write_geoparquet(
        con, core_select_sql, parquet_path, source.row_group_size, crs=source.crs
    )
    _log_prepared(parquet_path, elapsed)
    return parquet_path


def _prepare_parquet(
    source: ParquetSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    if _should_skip(parquet_path, force):
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Preparing %s from %s", name, source.path)

    con = _open_connection(threads)

    pq_src_meta = pq.read_metadata(str(source.path))
    log.info(
        "  Source: %s rows | %d row groups",
        f"{pq_src_meta.num_rows:,}",
        pq_src_meta.num_row_groups,
    )

    geom_col = source.geometry_col
    if source.source_crs is not None:
        # Native GEOMETRY column in a foreign CRS — reproject to target CRS.
        # always_xy=true forces lon/lat (x/y) axis order, overriding PROJ 6+'s
        # official axis order for EPSG:4326 (which is lat/lon). Without this,
        # DuckDB interprets the first coordinate as latitude, swapping axes and
        # producing completely wrong output coordinates.
        geom_expr = f"ST_Transform({geom_col}, '{_sql_str(source.source_crs)}', '{_sql_str(source.crs)}', always_xy := true)"
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
            f"DESCRIBE SELECT * FROM read_parquet('{_sql_str(source.path)}')"
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

    log.info("  Source geometry column: %r → output column: 'geometry'", geom_col)
    log.info("  Hilbert sort + write → %s ...", parquet_path)

    core_select_sql = f"""
        SELECT
            * EXCLUDE ({exclude_cols}),
            {geom_expr} AS geometry
        FROM read_parquet('{_sql_str(source.path)}')
    """
    elapsed = _write_geoparquet(
        con, core_select_sql, parquet_path, source.row_group_size, crs=source.crs
    )
    _log_prepared(parquet_path, elapsed)
    return parquet_path


def _prepare_usrn_buffer(
    source: UsrnSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    """Buffer an already-prepared USRN centreline GeoParquet for line join Phase 2.

    Private helper — only reachable with ``source.buffer_m`` set. Call
    ``_prepare_usrn`` instead, which dispatches correctly based on ``source.buffer_m``.
    """
    if source.buffer_m is None:
        raise ValueError(
            "_prepare_usrn_buffer requires UsrnSource.buffer_m to be set; got None. "
            "Call _prepare_usrn(source, ...) instead — it dispatches to the plain "
            "OGR path when buffer_m is None."
        )

    if _should_skip(parquet_path, force):
        return parquet_path

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    con = _open_connection(threads)

    pq_src = pq.read_metadata(str(source.path))
    log.info("Preparing %s (buffer=%.0fm) from %s", name, source.buffer_m, source.path)
    log.info(
        "  Source: %s rows | %d row groups",
        f"{pq_src.num_rows:,}",
        pq_src.num_row_groups,
    )
    log.info(
        "  Source geometry column: 'geometry' → output column: 'geometry' "
        "(buffered %.0fm; original line kept as 'geometry_line')",
        source.buffer_m,
    )
    log.info("  Hilbert sort + write → %s ...", parquet_path)

    # Materialise the buffer once here so the shared bbox struct (built against
    # the already-computed `geometry` column) doesn't recompute ST_Buffer per corner.
    core_select_sql = f"""
        SELECT
            usrn,
            street_type,
            geometry AS geometry_line,
            ST_Buffer(geometry, {source.buffer_m}) AS geometry
        FROM read_parquet('{_sql_str(source.path)}')
    """
    elapsed = _write_geoparquet(
        con,
        core_select_sql,
        parquet_path,
        source.row_group_size,
        crs=source.crs,
        primary_column="geometry",
    )
    _log_prepared(parquet_path, elapsed)
    return parquet_path


def _prepare_usrn(
    source: UsrnSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
) -> pathlib.Path:
    """Dispatch a UsrnSource to plain (centreline) or buffered (corridor) preparation.

    ``source.buffer_m is None`` — ``source.path`` is a raw OGR-readable USRN source;
    delegates to ``_prepare_ogr`` via a transient ``OgrSource`` built from the
    ``path``/``crs``/``row_group_size`` fields shared by both structs.

    ``source.buffer_m`` set — ``source.path`` is an already-prepared USRN centreline
    GeoParquet; delegates to ``_prepare_usrn_buffer`` to build the buffered corridor.
    """
    if source.buffer_m is None:
        ogr_source = OgrSource(
            path=source.path, crs=source.crs, row_group_size=source.row_group_size
        )
        return _prepare_ogr(ogr_source, parquet_path, name, force, threads)
    return _prepare_usrn_buffer(source, parquet_path, name, force, threads)


def prepare(
    config: DatasetConfig,
    force: bool = False,
    threads: int | None = None,
) -> pathlib.Path:
    """Read a spatial data source and write an optimised GeoParquet 1.1 file.

    Dispatches to the correct reader based on ``config.source`` type:

    - ``OgrSource`` — any GDAL-readable vector format (GeoPackage, Shapefile, …)
    - ``CsvSource`` — CSV with explicit x/y coordinate columns, or a WKT text column
    - ``ParquetSource`` — existing GeoParquet to re-sort and re-compress
    - ``UsrnSource`` — USRN prep: plain centrelines (``buffer_m=None``) or
      buffered corridors for line-join Phase 2 (``buffer_m=<float>``)

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
        case UsrnSource() as src:
            return _prepare_usrn(src, config.parquet_path, config.name, force, threads)
