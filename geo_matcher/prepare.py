import contextlib
import json
import logging
import pathlib
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any, TypedDict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS as ProjCRS

from .config import (
    CsvSource,
    DatasetConfig,
    GeometryType,
    OgrSource,
    ParquetSource,
    UprnSource,
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


class _OgrInfo(TypedDict):
    crs: str | None
    feature_count: int
    geometry_type: str


_EXPECTED_COVERING_METADATA: _Covering = {
    "bbox": {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }
}


_WKB_TYPE_NAMES: dict[int, str] = {
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
    8: "CircularString",
    9: "CompoundCurve",
    10: "CurvePolygon",
    11: "MultiCurve",
    12: "MultiSurface",
    13: "Curve",
    14: "Surface",
    15: "PolyhedralSurface",
    16: "TIN",
    17: "Triangle",
}


# HELPERS
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
    """Return the geometry column name as exposed by DuckDB's ``st_read``."""
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


def _should_skip(parquet_path: pathlib.Path, force: bool) -> bool:
    """Return True (after logging) if the output already exists and force=False."""
    if not force and parquet_path.exists():
        log.info(
            "GeoParquet already exists at %s — skipping. Pass force=True to re-prepare.",
            parquet_path,
        )
        return True
    return False


def _open_connection(
    threads: int | None = None, memory_limit: str | None = None
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the spatial extension loaded."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if threads is not None:
        con.execute(f"SET threads = {threads};")
    if memory_limit is not None:
        con.execute(f"SET memory_limit = '{_sql_str(memory_limit)}';")
    return con


@contextlib.contextmanager
def _connection(
    threads: int | None = None, memory_limit: str | None = None
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open a configured DuckDB connection and always close it."""
    con = _open_connection(threads, memory_limit)
    try:
        yield con
    finally:
        con.close()


def _read_ogr_info(con: duckdb.DuckDBPyConnection, source_path: str) -> _OgrInfo:
    """Read layer metadata for an OGR source using DuckDB's own GDAL."""
    rows = con.execute(f"""
        SELECT
            l.feature_count,
            g.type,
            g.crs.auth_name,
            g.crs.auth_code
        FROM st_read_meta('{_sql_str(source_path)}'),
             UNNEST(layers) AS _(l),
             UNNEST(l.geometry_fields) AS __(g)
        LIMIT 1
    """).fetchall()
    if not rows:
        raise ValueError(f"No layers with geometry found in {source_path!r}")

    feature_count, geometry_type, auth_name, auth_code = rows[0]
    crs = f"{auth_name}:{auth_code}" if auth_name and auth_code else None
    return {
        "crs": crs,
        "feature_count": feature_count if feature_count is not None else -1,
        "geometry_type": geometry_type or "unknown",
    }


def _bbox_struct_sql(geom_col: str = "geometry") -> str:
    """Return the ``{'xmin': ..., ...} AS bbox``-ready struct expression for ``geom_col``."""
    return (
        "{"
        f"'xmin': ST_XMin({geom_col}), 'ymin': ST_YMin({geom_col}), "
        f"'xmax': ST_XMax({geom_col}), 'ymax': ST_YMax({geom_col})"
        "}"
    )


_BNG_EPSG = "EPSG:27700"
_BNG_PROJ4_TEMPLATE = (
    "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 "
    "+ellps=airy +units=m +no_defs +nadgrids={nadgrids_path} +type=crs"
)


def _crs_arg(crs: str, nadgrids_path: pathlib.Path | None) -> str:
    """Return the CRS argument to hand ``ST_Transform`` for ``crs``.

    Plain EPSG codes go through DuckDB's own default coordinate operation
    search, which DuckDB's docs say can be off by "about 10m and possibly much
    more" for EPSG:27700. The only way to get OSTN15/NTv2 accuracy is to embed
    a ``+nadgrids=`` PROJ4 pipeline directly, in place of the plain EPSG code —
    there's no "grid installed" state to detect instead (DuckDB vendors its own
    static PROJ, separate from any system/pyproj install, and exposes no way to
    query its operation/grid choice).
    """
    if nadgrids_path is not None and crs.upper() == _BNG_EPSG:
        if not nadgrids_path.exists():
            raise ValueError(
                f"nadgrids_path {nadgrids_path} does not exist — download and "
                "extract the OS OSTN15 NTv2 format files ZIP (contains the "
                "binary .gsb grid, a user guide, and samples) from "
                "https://www.ordnancesurvey.co.uk/products/os-net/for-developers "
                "and point nadgrids_path at the .gsb file inside it "
                "(e.g. OSTN15_NTv2_OSGBtoETRS.gsb)."
            )
        return _BNG_PROJ4_TEMPLATE.format(nadgrids_path=nadgrids_path)
    return crs


def _crs_transform_expr(
    geom_expr: str,
    source_crs: str | None,
    target_crs: str,
    nadgrids_path: pathlib.Path | None = None,
) -> str:
    """Wrap ``geom_expr`` in ``ST_Transform`` if ``source_crs`` is set and differs
    from ``target_crs``; otherwise return it unchanged.

    ``always_xy=true`` forces lon/lat (x/y) axis order, matching the one existing
    transform call this generalises (formerly inline in ``_prepare_parquet``).
    """
    if source_crs is None or source_crs == target_crs:
        return geom_expr
    source_arg = _crs_arg(source_crs, nadgrids_path)
    target_arg = _crs_arg(target_crs, nadgrids_path)
    return (
        f"ST_Transform({geom_expr}, '{_sql_str(source_arg)}', "
        f"'{_sql_str(target_arg)}', always_xy := true)"
    )


def _crs_transform_note(
    source_crs: str | None, target_crs: str, nadgrids_path: pathlib.Path | None
) -> str:
    """Flag, deterministically, whether this transform has an OSTN15/NTv2 grid
    configured — not by querying any PROJ library (DuckDB vendors its own,
    separate from pyproj's; neither exposes operation/grid introspection), but
    from the plain fact of whether ``nadgrids_path`` was set for a transform
    touching EPSG:27700 (see ``_crs_arg``).

    Returns a short suffix for the caller's CRS log line; ``""`` if no
    transform is needed or it doesn't touch EPSG:27700.
    """
    if source_crs is None or source_crs == target_crs:
        return ""
    if _BNG_EPSG not in (source_crs.upper(), target_crs.upper()):
        return ""
    if nadgrids_path is not None:
        return f" [OSTN15 grid: {nadgrids_path}]"
    log.warning(
        "  CRS %s → %s: no NTv2/OSTN15 grid configured — DuckDB's default "
        "coordinate operation for EPSG:27700 can be off by 10m or more (see "
        "DuckDB's ST_Transform docs). Download and extract the OS OSTN15 "
        "NTv2 format files ZIP "
        "(https://www.ordnancesurvey.co.uk/products/os-net/for-developers) "
        "and set nadgrids_path to the .gsb file inside it for accurate "
        "results.",
        source_crs,
        target_crs,
    )
    return " [no OSTN15 grid configured]"


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


def _wkb_type_name(prefix_hex: str) -> str:
    """Name the geometry type encoded in the first five bytes of a WKB value.

    ``prefix_hex`` is the hex of ``[byte order][uint32 type code]``. Both WKB
    dialects GDAL emits are handled: ISO (``1002`` = LineString Z) and the older
    OGC/EWKB high-bit flags (``0x80000002``).
    """
    try:
        raw = bytes.fromhex(prefix_hex)
    except ValueError:
        return "unknown"
    if len(raw) < 5:
        return "unknown"

    code = int.from_bytes(raw[1:5], "little" if raw[0] == 1 else "big")

    # EWKB signals extra dimensions with high bits; ISO adds 1000/2000/3000.
    has_z = bool(code & 0x80000000)
    has_m = bool(code & 0x40000000)
    base = code & 0x0FFFFFFF
    if base >= 1000:
        base, marker = base % 1000, base // 1000
        has_z = has_z or marker in (1, 3)
        has_m = has_m or marker in (2, 3)

    suffix = f" {'Z' if has_z else ''}{'M' if has_m else ''}".rstrip()
    return f"{_WKB_TYPE_NAMES.get(base, f'type {code}')}{suffix}"


def _log_dropped_geometries(
    con: duckdb.DuckDBPyConnection,
    staged_path: pathlib.Path,
    source_path: pathlib.Path,
    staged_rows: int,
    written_rows: int,
) -> None:
    """Warn about features that could not be converted out of the staging file."""
    dropped = staged_rows - written_rows
    if dropped <= 0:
        return

    row = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE _geom_wkb IS NULL),
            list(DISTINCT hex(_geom_wkb)[1:10]) FILTER (
                WHERE _geom_wkb IS NOT NULL
                  AND TRY(ST_GeomFromWKB(_geom_wkb)) IS NULL
            )
        FROM read_parquet('{_sql_str(staged_path)}')
    """).fetchone()
    null_geoms: int = row[0] if row else 0
    prefixes: list[str] = (row[1] if row and row[1] else []) or []

    if null_geoms:
        log.warning(
            "  %s of %s features in %s have no geometry and were dropped.",
            f"{null_geoms:,}",
            f"{staged_rows:,}",
            source_path,
        )

    unsupported = dropped - null_geoms
    if unsupported > 0:
        types = ", ".join(sorted({_wkb_type_name(p) for p in prefixes})) or "unknown"
        log.warning(
            "  %s of %s features (%.4f%%) in %s use geometry DuckDB cannot "
            "represent (%s) and were dropped. Pre-convert the source to keep them: "
            "ogr2ogr -nlt CONVERT_TO_LINEAR converted.gpkg %s",
            f"{unsupported:,}",
            f"{staged_rows:,}",
            100 * unsupported / staged_rows if staged_rows else 0.0,
            source_path,
            types,
            source_path,
        )


@dataclass
class _PreparedOutput:
    """What a ``_prepare_*`` dispatcher's ``build(con)`` closure hands back to
    ``_prepare_common`` to finish the job."""

    core_select_sql: str
    row_group_size: int
    crs: str | None = None
    primary_column: str | None = None
    post_write: Callable[[duckdb.DuckDBPyConnection], None] | None = None


def _log_duckdb_proj_version(con: duckdb.DuckDBPyConnection) -> None:
    """Log the PROJ version DuckDB's spatial extension is actually compiled
    against — separate from, and not necessarily the same as, any system or
    pyproj-bundled PROJ (see ``_crs_arg``/``_crs_transform_note``, which had to
    stop assuming those match). Logged once per prepare run so a divergence
    between environments is visible without having to query it manually.
    """
    row = con.sql("SELECT DuckDB_PROJ_Compiled_Version()").fetchone()
    log.info("  DuckDB PROJ: %s", row[0] if row else "unknown")


def _prepare_common(
    parquet_path: pathlib.Path,
    force: bool,
    threads: int | None,
    memory_limit: str | None,
    build: Callable[[duckdb.DuckDBPyConnection], _PreparedOutput],
) -> pathlib.Path:
    """Shared skeleton every ``_prepare_*`` dispatcher follows: should_skip -> mkdir
    -> open connection -> build() the source-specific SELECT -> write -> log -> return.

    ``build`` does everything source-specific — reading/validating source metadata,
    logging what it found, and returning the ``core_select_sql`` (+ ``crs``/
    ``primary_column`` for ``_write_geoparquet``, + an optional ``post_write`` hook
    for diagnostics that must run after the file is written, e.g. OGR's
    dropped-geometry report).
    """
    if _should_skip(parquet_path, force):
        return parquet_path
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    with _connection(threads, memory_limit) as con:
        _log_duckdb_proj_version(con)
        built = build(con)
        elapsed = _write_geoparquet(
            con,
            built.core_select_sql,
            parquet_path,
            built.row_group_size,
            crs=built.crs,
            primary_column=built.primary_column,
        )
        if built.post_write is not None:
            built.post_write(con)
        _log_prepared(parquet_path, elapsed)
        return parquet_path


# PREPARE DISPATCHERS
def _prepare_ogr(
    source: OgrSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
    columns_sql: str | None = None,
) -> pathlib.Path:
    """Prepare an OGR-readable source (GeoPackage, Shapefile, etc) as a
    hilbert sorted GeoParquet.

    The OGR read is staged to a plain Parquet file first and then is hilbert sorted.

    The staging read uses ``keep_wkb=true``.

    Geometry is forced to 2D.

    ``columns_sql`` overrides the non-geometry projection, which defaults to every
    source column but the geometry. It is applied to the *staging* SELECT rather
    than the rebuild so unwanted columns never reach the staging file at all —
    ``_prepare_uprn`` uses it to keep only the id, and at 40M+ rows the
    columns it drops are four doubles per row.
    """
    staged_path = parquet_path.with_name(f"{parquet_path.stem}.staging.parquet")

    def build(con: duckdb.DuckDBPyConnection) -> _PreparedOutput:
        info = _read_ogr_info(con, str(source.path))
        detected_crs = info["crs"]
        if detected_crs is None:
            raise ValueError(
                f"Could not detect source CRS for {source.path} — cannot verify "
                f"or transform to expected CRS {source.crs}."
            )
        feature_count = info["feature_count"]
        crs_desc = (
            detected_crs
            if detected_crs == source.crs
            else f"detected {detected_crs} → target {source.crs}"
            f"{_crs_transform_note(detected_crs, source.crs, source.nadgrids_path)}"
        )
        log.info(
            "  CRS: %s | features: %s | geometry: %s",
            crs_desc,
            f"{feature_count:,}" if feature_count >= 0 else "unknown",
            info["geometry_type"],
        )

        src_geom: str = _get_src_geometry_col(con, str(source.path))
        log.info("  Source geometry column: %r → output column: 'geometry'", src_geom)
        staged_columns_sql = columns_sql or f'* EXCLUDE "{src_geom}"'

        log.info("  Staging OGR source → %s ...", staged_path)
        con.execute(f"""
            COPY (
                SELECT
                    {staged_columns_sql},
                    "{src_geom}" AS _geom_wkb
                FROM st_read('{_sql_str(source.path)}', keep_wkb=true)
            ) TO '{_sql_str(staged_path)}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        staged_rows: int = pq.read_metadata(str(staged_path)).num_rows

        log.info("  Hilbert sort + write → %s ...", parquet_path)
        # Ensure geometries are 2D only here.
        geom_expr = _crs_transform_expr(
            "ST_Force2D(TRY(ST_GeomFromWKB(_geom_wkb)))",
            detected_crs,
            source.crs,
            source.nadgrids_path,
        )
        core_select_sql = f"""
            SELECT * FROM (
                SELECT
                    * EXCLUDE _geom_wkb,
                    {geom_expr} AS geometry
                FROM read_parquet('{_sql_str(staged_path)}')
            )
            WHERE geometry IS NOT NULL
        """

        def post_write(con: duckdb.DuckDBPyConnection) -> None:
            _log_dropped_geometries(
                con,
                staged_path,
                source.path,
                staged_rows,
                pq.read_metadata(str(parquet_path)).num_rows,
            )

        return _PreparedOutput(
            core_select_sql,
            source.row_group_size,
            crs=source.crs,
            post_write=post_write,
        )

    try:
        return _prepare_common(parquet_path, force, threads, memory_limit, build)
    finally:
        staged_path.unlink(missing_ok=True)


def _prepare_csv(
    source: CsvSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Prepare CSV source"""

    def build(con: duckdb.DuckDBPyConnection) -> _PreparedOutput:
        geom_sql, exclude_sql = _csv_geometry_sql(source)
        geom_sql = _crs_transform_expr(
            geom_sql, source.source_crs, source.crs, source.nadgrids_path
        )
        crs_desc = (
            source.crs
            if not source.source_crs or source.source_crs == source.crs
            else f"declared {source.source_crs} → target {source.crs}"
            f"{_crs_transform_note(source.source_crs, source.crs, source.nadgrids_path)}"
        )
        log.info(
            "  Source geometry: %s → output column: 'geometry' | CRS: %s",
            geom_sql,
            crs_desc,
        )

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
        return _PreparedOutput(core_select_sql, source.row_group_size, crs=source.crs)

    return _prepare_common(parquet_path, force, threads, memory_limit, build)


def _prepare_parquet(
    source: ParquetSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Pepare a parquet file as a hilbert sorted geoparquet."""

    def build(con: duckdb.DuckDBPyConnection) -> _PreparedOutput:
        pq_src_meta = pq.read_metadata(str(source.path))
        log.info(
            "  Source: %s rows | %d row groups",
            f"{pq_src_meta.num_rows:,}",
            pq_src_meta.num_row_groups,
        )

        geom_col = source.geometry_col
        if source.source_crs is not None:
            # Native GEOMETRY column in a foreign CRS — reproject to target CRS.
            geom_expr = _crs_transform_expr(
                geom_col, source.source_crs, source.crs, source.nadgrids_path
            )
            log.info(
                "  CRS: declared %s → target %s%s",
                source.source_crs,
                source.crs,
                _crs_transform_note(
                    source.source_crs, source.crs, source.nadgrids_path
                ),
            )
        elif geom_col == "geometry":
            # WKB blob written by this pipeline — must promote to GEOMETRY explicitly.
            geom_expr = "ST_GeomFromWKB(geometry)"
        else:
            # Native GEOMETRY column already in the target CRS (no reprojection needed).
            geom_expr = geom_col

        # Build the EXCLUDE list for the inner SELECT.
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
        return _PreparedOutput(core_select_sql, source.row_group_size, crs=source.crs)

    return _prepare_common(parquet_path, force, threads, memory_limit, build)


def _prepare_usrn_buffer(
    source: UsrnSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Buffer an already-prepared USRN centreline GeoParquet for line join Phase 2.

    Private helper — only reachable with ``source.buffer_m`` set. Call
    ``_prepare_usrn`` instead, which dispatches correctly based on ``source.buffer_m``.
    """

    if not isinstance(source, UsrnSource):
        raise ValueError("Source is of incorrect type")

    if source.buffer_m is None:
        raise ValueError(
            "_prepare_usrn_buffer requires UsrnSource.buffer_m to be set; got None. "
            "Call _prepare_usrn(source, ...) instead — it dispatches to the plain "
            "OGR path when buffer_m is None."
        )

    def build(con: duckdb.DuckDBPyConnection) -> _PreparedOutput:
        pq_src = pq.read_metadata(str(source.path))
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
        return _PreparedOutput(
            core_select_sql,
            source.row_group_size,
            crs=source.crs,
            primary_column="geometry",
        )

    return _prepare_common(parquet_path, force, threads, memory_limit, build)


def _prepare_usrn(
    source: UsrnSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Dispatch a UsrnSource to normal or buffered file.

    ``source.buffer_m is None`` — ``source.path`` is a raw OGR-readable USRN source;
    delegates to ``_prepare_ogr`` via a transient ``OgrSource`` built from the
    ``path``/``crs``/``row_group_size`` fields shared by both structs.

    ``source.buffer_m`` set — ``source.path`` is an already-prepared USRN centreline
    GeoParquet; delegates to ``_prepare_usrn_buffer`` to build the buffered corridor.
    """
    if not isinstance(source, UsrnSource):
        raise ValueError("Source is of incorrect type")

    if source.buffer_m is None:
        ogr_source = OgrSource(
            path=source.path,
            crs=source.crs,
            row_group_size=source.row_group_size,
            nadgrids_path=source.nadgrids_path,
        )
        return _prepare_ogr(
            ogr_source, parquet_path, name, force, threads, memory_limit
        )
    return _prepare_usrn_buffer(
        source, parquet_path, name, force, threads, memory_limit
    )


def _prepare_uprn_buffer(
    source: UprnSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Buffer an already-prepared UPRN point GeoParquet into buffered polygons.

    Private helper — only reachable with ``source.buffer_m`` set. Call
    ``_prepare_uprn`` instead, which dispatches correctly based on
    ``source.buffer_m``.
    """
    if not isinstance(source, UprnSource):
        raise ValueError("Source is of incorrect type")

    if source.buffer_m is None:
        raise ValueError(
            "_prepare_uprn_buffer requires UprnSource.buffer_m to be set; got None. "
            "Call _prepare_uprn(source, ...) instead — it dispatches to the plain "
            "OGR-derived path when buffer_m is None."
        )

    def build(con: duckdb.DuckDBPyConnection) -> _PreparedOutput:
        pq_src = pq.read_metadata(str(source.path))
        log.info(
            "  Source: %s rows | %d row groups",
            f"{pq_src.num_rows:,}",
            pq_src.num_row_groups,
        )
        log.info(
            "  Source geometry column: 'geometry' → output column: 'geometry' "
            "(buffered %.0fm; original point kept as 'geometry_point')",
            source.buffer_m,
        )
        log.info("  Hilbert sort + write → %s ...", parquet_path)

        # Only uprn carries through — x/y/lat/lon are redundant with geometry_point
        # and this file's row count is large enough that dropping four doubles per
        # row is worth it.
        core_select_sql = f"""
            SELECT
                uprn,
                geometry AS geometry_point,
                ST_Buffer(geometry, {source.buffer_m}) AS geometry
            FROM read_parquet('{_sql_str(source.path)}')
        """
        return _PreparedOutput(
            core_select_sql,
            source.row_group_size,
            crs=source.crs,
            primary_column="geometry",
        )

    return _prepare_common(parquet_path, force, threads, memory_limit, build)


def _prepare_uprn(
    source: UprnSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Dispatch a UprnSource to normal or buffered file.

    ``source.buffer_m is None`` — ``source.path`` is a raw OGR-readable UPRN
    source; delegates to ``_prepare_ogr`` via a transient ``OgrSource`` built from
    the ``path``/``crs``/``row_group_size`` fields shared by both structs. Only
    ``uprn`` and ``geometry`` are carried through — the source's uppercase
    ``UPRN`` id is renamed to match this pipeline's lowercase convention.

    ``source.buffer_m`` set — ``source.path`` is an already-prepared plain
    UPRN GeoParquet; delegates to ``_prepare_uprn_buffer`` to build the
    buffered catchment polygons.
    """
    if not isinstance(source, UprnSource):
        raise ValueError("Source is of incorrect type")

    if source.buffer_m is None:
        ogr_source = OgrSource(
            path=source.path,
            crs=source.crs,
            row_group_size=source.row_group_size,
            nadgrids_path=source.nadgrids_path,
        )
        return _prepare_ogr(
            ogr_source,
            parquet_path,
            name,
            force,
            threads,
            memory_limit,
            columns_sql="UPRN AS uprn",
        )
    return _prepare_uprn_buffer(
        source, parquet_path, name, force, threads, memory_limit
    )


def prepare(
    config: DatasetConfig,
    force: bool = False,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Read a spatial data source and write an optimised GeoParquet 1.1 file.

    Dispatches to the correct reader based on ``config.source`` type:

    - ``OgrSource`` — any GDAL-readable vector format (GeoPackage, Shapefile, …)
    - ``CsvSource`` — CSV with explicit x/y coordinate columns, or a WKT text column
    - ``ParquetSource`` — existing GeoParquet to re-sort and re-compress
    - ``UsrnSource`` — USRN prep: plain centrelines (``buffer_m=None``) or
      buffered corridors for line-join Phase 2 (``buffer_m=<float>``)
    - ``UprnSource`` — UPRN prep: plain address points (``buffer_m=None``) or
      buffered catchment polygons (``buffer_m=<float>``)

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
    memory_limit:
        DuckDB ``memory_limit`` for this call, e.g. ``"3GB"``. ``None`` leaves
        DuckDB's default of ~80% of system RAM. That default is applied *per
        instance*, so on a memory-constrained runner set this explicitly.

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
    log.info("Processing %s", config.source)
    match config.source:
        case OgrSource() as src:
            return _prepare_ogr(
                src, config.parquet_path, config.name, force, threads, memory_limit
            )
        case CsvSource() as src:
            return _prepare_csv(
                src, config.parquet_path, config.name, force, threads, memory_limit
            )
        case ParquetSource() as src:
            return _prepare_parquet(
                src, config.parquet_path, config.name, force, threads, memory_limit
            )
        case UsrnSource() as src:
            return _prepare_usrn(
                src, config.parquet_path, config.name, force, threads, memory_limit
            )
        case UprnSource() as src:
            return _prepare_uprn(
                src, config.parquet_path, config.name, force, threads, memory_limit
            )
