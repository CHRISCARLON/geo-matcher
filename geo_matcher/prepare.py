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
_BNG_EPSG = "EPSG:27700"
_OGC_CRS84 = (
    "OGC:CRS84"  # GeoParquet spec's default CRS when a column entry omits "crs"
)
_BNG_PROJ4_TEMPLATE = (
    "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 "
    "+ellps=airy +units=m +no_defs +nadgrids={nadgrids_path} +type=crs"
)

_LONLAT_EPSG = "EPSG:4326"
# GB + NI lon/lat envelope, used by _detect_csv_crs. Tight on purpose so
# small-magnitude local/planar coordinates (e.g. toy test data near the
# origin) aren't misread as WGS84.
_GB_LONLAT_ENVELOPE = "ST_MakeEnvelope(-9.0, 49.5, 2.5, 61.0)"

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


class _CoveringColumn(TypedDict):
    xmin: list[str]
    ymin: list[str]
    xmax: list[str]
    ymax: list[str]


class _Covering(TypedDict):
    bbox: _CoveringColumn


_EXPECTED_COVERING_METADATA: _Covering = {
    "bbox": {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }
}


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


# HELPERS
def _sql_str(value: object) -> str:
    """Escape a value for safe interpolation inside a single-quoted SQL string literal.

    DuckDB paths/CRS strings are inlined into SQL (no parameter binding in ``COPY``
    statements), so a value containing ``'`` would otherwise break or inject the query.
    """
    return str(value).replace("'", "''")


def _patch_covering_metadata(
    path: pathlib.Path,
    row_group_size: int,
    crs: str | None = None,
    primary_column: str | None = None,
) -> None:
    """Patch a GeoParquet file's metadata to add the GeoParquet 1.1 covering key,
    the CRS, and normalised types — DuckDB's own ``COPY ... TO ... (FORMAT
    PARQUET)`` writes plain 1.0.0 metadata with neither. Rewrites the file
    in-place with ZSTD compression.
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
    geometry_type: GeometryType = GeometryType(source.geometry_type)
    match geometry_type:
        case GeometryType.POINT:
            return (
                f'ST_Point("{source.x_col}", "{source.y_col}")',
                f'"{source.x_col}", "{source.y_col}"',
            )
        case GeometryType.LINE | GeometryType.POLYGON:
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
    """Read layer metadata for an OGR source using DuckDB."""
    rows = con.execute(f"""
        SELECT
            layer.feature_count,
            geo.type,
            geo.crs.auth_name,
            geo.crs.auth_code
        FROM st_read_meta('{_sql_str(source_path)}'),
             UNNEST(layers) AS _(layer),
             UNNEST(layer.geometry_fields) AS __(geo)
        LIMIT 1
    """).fetchall()
    log.debug(f"OGR metadata fields: {rows}")
    if not rows:
        raise ValueError(f"No layers with geometry found in {source_path!r}")

    feature_count, geometry_type, auth_name, auth_code = rows[0]
    crs = f"{auth_name}:{auth_code}" if auth_name and auth_code else None
    return {
        "crs": crs,
        "feature_count": feature_count if feature_count is not None else -1,
        "geometry_type": geometry_type or "unknown",
    }


def _read_parquet_geo_crs(
    schema_metadata: dict[bytes, bytes] | None, geom_col: str
) -> str | None:
    """Read a GeoParquet file's declared CRS for ``geom_col`` from its ``geo``
    metadata — the Parquet-side analogue of ``_read_ogr_info``'s CRS detection.

    Returns ``None`` if there's no ``geo`` metadata or ``geom_col`` isn't listed.
    Returns ``"OGC:CRS84"`` (the GeoParquet spec's default) if listed but the
    entry omits ``crs``. Otherwise resolves the embedded PROJJSON to
    ``"AUTHORITY:CODE"``, or its WKT if no authority code exists.
    """
    if not schema_metadata or b"geo" not in schema_metadata:
        return None
    geo_meta: _GeoMeta = json.loads(schema_metadata[b"geo"])
    col_meta = geo_meta.get("columns", {}).get(geom_col)
    if col_meta is None:
        return None
    crs_json = col_meta.get("crs")
    if crs_json is None:
        return _OGC_CRS84
    crs = ProjCRS.from_json_dict(crs_json)
    authority = crs.to_authority()
    return f"{authority[0]}:{authority[1]}" if authority else crs.to_wkt()


def _detect_csv_crs(
    con: duckdb.DuckDBPyConnection, source: CsvSource, geom_sql: str
) -> str | None:
    """Best-effort CRS detection for a CSV/WKT source, from coordinate extent.

    CSVs carry no CRS metadata, but WGS84 lon/lat and British National Grid
    eastings/northings occupy distinct areas over Great Britain, so testing
    the data's own extent against each candidate envelope directly in DuckDB
    is usually enough to tell them apart.
    """

    row = con.sql(f"""
        SELECT
            ST_Within(ST_Extent_Agg(g), {_GB_LONLAT_ENVELOPE}),
            ST_Within(ST_Extent_Agg(g), {_BNG_BOX}::GEOMETRY)
        FROM (
            SELECT {geom_sql} AS g
            FROM read_csv('{_sql_str(source.path)}', auto_detect=true, null_padding=true, nullstr=['NULL', ''])
        ) WHERE g IS NOT NULL
    """).fetchone()
    if row is None:
        return None
    is_lonlat, is_bng = row
    if is_lonlat:
        log.debug(f"CSV is: {_LONLAT_EPSG}")
        return _LONLAT_EPSG
    if is_bng:
        log.debug(f"CSV is: {_BNG_EPSG}")
        return _BNG_EPSG
    return None


def _bbox_struct_sql(geom_col: str = "geometry") -> str:
    """Return the ``{'xmin': ..., ...} AS bbox`` ready struct expression for ``geom_col``."""
    return (
        "{"
        f"'xmin': ST_XMin({geom_col}), 'ymin': ST_YMin({geom_col}), "
        f"'xmax': ST_XMax({geom_col}), 'ymax': ST_YMax({geom_col})"
        "}"
    )


def _crs_arg(crs: str, nadgrids_path: pathlib.Path | None) -> str:
    """Return the CRS argument to hand ``ST_Transform`` for ``crs``.

    Plain EPSG:27700 goes through DuckDB's default coordinate operation, which
    can be off by 10m or more. The only way to get OSTN15/NTv2 accuracy is to
    embed a ``+nadgrids=`` PROJ4 pipeline in place of the EPSG code — DuckDB
    exposes no "grid installed" state to detect instead.
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
    from ``target_crs``; otherwise return it unchanged. ``always_xy=true`` forces
    lon/lat (x/y) axis order.
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
    """Flag whether this transform has an OSTN15/NTv2 grid configured."""
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


def _crs_equal(crs_a: str, crs_b: str) -> bool:
    """Compare two CRS strings for equivalence, tolerant of differing forms
    (``"EPSG:27700"`` vs ``"27700"`` vs WKT).

    Falls back to a case-insensitive string comparison if either value can't
    be parsed by pyproj — ``_read_parquet_geo_crs`` can return raw WKT for a
    CRS with no authority code.
    """
    try:
        return ProjCRS.from_user_input(crs_a) == ProjCRS.from_user_input(crs_b)
    except Exception:
        return crs_a.strip().upper() == crs_b.strip().upper()


def _warn_crs_mismatch(
    declared_crs: str | None, detected_crs: str, source_path: pathlib.Path
) -> None:
    """Warn when a user-declared ``source_crs`` disagrees with the CRS actually
    detected in the file. The detected CRS always wins for the transform —
    this only flags the disagreement.
    """
    if declared_crs is None or _crs_equal(declared_crs, detected_crs):
        return
    log.warning(
        "  %s: declared source_crs=%s but detected %s from the file — using "
        "the detected CRS for the transform (declared value ignored).",
        source_path,
        declared_crs,
        detected_crs,
    )


def _crs_log_desc(
    source_crs: str | None,
    target_crs: str,
    nadgrids_path: pathlib.Path | None,
    verb: str,
) -> str:
    """Build the CRS clause shared by every ``_prepare_*`` dispatcher's log line.

    Returns ``target_crs`` unchanged when no transform will happen; otherwise
    ``"<verb> <source_crs> → target <target_crs>[OSTN15 note]"``, via
    ``_crs_transform_note`` for the bracketed suffix.
    """
    if source_crs is None or source_crs == target_crs:
        return target_crs
    return (
        f"{verb} {source_crs} → target {target_crs}"
        f"{_crs_transform_note(source_crs, target_crs, nadgrids_path)}"
    )


def _write_geoparquet(
    con: duckdb.DuckDBPyConnection,
    core_select_sql: str,
    parquet_path: pathlib.Path,
    row_group_size: int,
    crs: str | None = None,
    primary_column: str | None = None,
) -> float:
    """Wrap ``core_select_sql`` with the bbox covering struct + Hilbert sort, write
    it as GeoParquet, and patch the covering/CRS metadata.

    ``core_select_sql`` must already produce a materialised ``geometry`` column —
    this only adds the bbox struct and ``ORDER BY ST_Hilbert(...)`` around it.
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
class _StagedOutput:
    """What a ``_prepare_*`` dispatcher's ``build(con)`` closure hands back to
    ``_prepare_common`` to finish the job."""

    core_select_sql: str
    row_group_size: int
    crs: str | None = None
    primary_column: str | None = None
    post_write: Callable[[duckdb.DuckDBPyConnection], None] | None = None


def _log_duckdb_proj_version(con: duckdb.DuckDBPyConnection) -> None:
    """Log the PROJ version DuckDB's spatial extension is using"""
    row = con.sql("SELECT DuckDB_PROJ_Compiled_Version()").fetchone()
    log.info("  DuckDB PROJ: %s", row[0] if row else "unknown")


def _prepare_common(
    parquet_path: pathlib.Path,
    force: bool,
    threads: int | None,
    memory_limit: str | None,
    build: Callable[[duckdb.DuckDBPyConnection], _StagedOutput],
) -> pathlib.Path:
    """Shared skeleton every ``_prepare_*`` dispatcher follows: should_skip -> mkdir
    -> open connection -> build() the source-specific SELECT -> write -> log -> return.

    ``build`` returns ``core_select_sql`` plus ``crs``/``primary_column`` for
    ``_write_geoparquet``, and an optional ``post_write`` hook for diagnostics
    that must run after the file is written (e.g. OGR's dropped-geometry report).
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
    Hilbert-sorted GeoParquet: staged to a plain Parquet file first
    (``keep_wkb=true``), then Hilbert-sorted with geometry forced to 2D.

    ``columns_sql`` overrides the non-geometry projection (default: every
    source column but geometry), applied to the staging SELECT so unwanted
    columns never reach the staging file — ``_prepare_uprn`` uses this to
    keep only the id, dropping four doubles per row at 40M+ rows.
    """
    staged_path = parquet_path.with_name(f"{parquet_path.stem}.staging.parquet")

    def build(con: duckdb.DuckDBPyConnection) -> _StagedOutput:
        info = _read_ogr_info(con, str(source.path))
        detected_crs = info["crs"]
        if detected_crs is None:
            raise ValueError(
                f"Could not detect source CRS for {source.path} — cannot verify "
                f"or transform to expected CRS {source.target_crs}."
            )
        _warn_crs_mismatch(source.source_crs, detected_crs, source.path)
        feature_count = info["feature_count"]
        crs_desc = _crs_log_desc(
            detected_crs, source.target_crs, source.nadgrids_path, verb="detected"
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
            source.target_crs,
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

        return _StagedOutput(
            core_select_sql,
            source.row_group_size,
            crs=source.target_crs,
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

    def build(con: duckdb.DuckDBPyConnection) -> _StagedOutput:
        geom_sql, exclude_sql = _csv_geometry_sql(source)
        detected_crs = _detect_csv_crs(con, source, geom_sql)
        if detected_crs is not None:
            _warn_crs_mismatch(source.source_crs, detected_crs, source.path)
        effective_crs = detected_crs if detected_crs is not None else source.source_crs
        geom_sql = _crs_transform_expr(
            geom_sql, effective_crs, source.target_crs, source.nadgrids_path
        )
        crs_desc = _crs_log_desc(
            effective_crs,
            source.target_crs,
            source.nadgrids_path,
            verb="detected" if detected_crs is not None else "declared",
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

        core_select_sql = f"""
            SELECT
                * EXCLUDE ({exclude_sql}),
                {geom_sql} AS geometry
            FROM read_csv('{_sql_str(source.path)}', auto_detect=true, null_padding=true, nullstr=['NULL', ''])
        """
        return _StagedOutput(
            core_select_sql, source.row_group_size, crs=source.target_crs
        )

    return _prepare_common(parquet_path, force, threads, memory_limit, build)


def _prepare_parquet(
    source: ParquetSource,
    parquet_path: pathlib.Path,
    name: str,
    force: bool,
    threads: int | None = None,
    memory_limit: str | None = None,
) -> pathlib.Path:
    """Prepare a Parquet file as a Hilbert-sorted GeoParquet."""

    def build(con: duckdb.DuckDBPyConnection) -> _StagedOutput:
        pq_src_meta = pq.read_metadata(str(source.path))
        log.info(
            "  Source: %s rows | %d row groups",
            f"{pq_src_meta.num_rows:,}",
            pq_src_meta.num_row_groups,
        )

        geom_col = source.geometry_col

        detected_crs = _read_parquet_geo_crs(pq_src_meta.metadata, geom_col)
        if detected_crs is None:
            raise ValueError(
                f"Could not detect source CRS for column {geom_col!r} in "
                f"{source.path} (no GeoParquet 'geo' metadata found) — cannot "
                f"verify or transform to expected CRS {source.target_crs}."
            )

        src_describe = con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{_sql_str(source.path)}')"
        ).fetchall()
        src_col_types = {row[0]: row[1] for row in src_describe}
        already_geometry_typed = "GEOMETRY" in src_col_types[geom_col].upper()
        extra_geom_cols = [
            name
            for name, typ in src_col_types.items()
            if "GEOMETRY" in typ.upper() and name != geom_col
        ]
        cols_to_exclude = [geom_col] + extra_geom_cols
        if "bbox" in src_col_types:
            cols_to_exclude.append("bbox")
        exclude_cols = ", ".join(f'"{c}"' for c in cols_to_exclude)
        if extra_geom_cols:
            log.info("  Excluding extra geometry columns: %s", extra_geom_cols)
        geom_expr = (
            geom_col if already_geometry_typed else f"ST_GeomFromWKB({geom_col})"
        )

        _warn_crs_mismatch(source.source_crs, detected_crs, source.path)
        geom_expr = _crs_transform_expr(
            geom_expr, detected_crs, source.target_crs, source.nadgrids_path
        )
        log.info(
            "  CRS: %s",
            _crs_log_desc(
                detected_crs, source.target_crs, source.nadgrids_path, verb="detected"
            ),
        )
        log.info("  Source geometry column: %r → output column: 'geometry'", geom_col)
        log.info("  Hilbert sort + write → %s ...", parquet_path)

        core_select_sql = f"""
            SELECT
                * EXCLUDE ({exclude_cols}),
                {geom_expr} AS geometry
            FROM read_parquet('{_sql_str(source.path)}')
        """
        return _StagedOutput(
            core_select_sql, source.row_group_size, crs=source.target_crs
        )

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

    def build(con: duckdb.DuckDBPyConnection) -> _StagedOutput:
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
        return _StagedOutput(
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

    ``buffer_m is None`` — raw OGR-readable USRN source; delegates to
    ``_prepare_ogr`` via a transient ``OgrSource``. ``buffer_m`` set —
    already-prepared USRN centreline GeoParquet; delegates to
    ``_prepare_usrn_buffer`` to build the buffered corridor.
    """

    if not isinstance(source, UsrnSource):
        raise ValueError("Source is of incorrect type")

    if source.buffer_m is None:
        ogr_source = OgrSource(
            path=source.path,
            target_crs=source.crs,
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

    def build(con: duckdb.DuckDBPyConnection) -> _StagedOutput:
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
        return _StagedOutput(
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

    ``buffer_m is None`` — raw OGR-readable UPRN source; delegates to
    ``_prepare_ogr`` via a transient ``OgrSource``, keeping only ``uprn``
    (renamed from the source's uppercase ``UPRN``) and ``geometry``.
    ``buffer_m`` set — already-prepared UPRN GeoParquet; delegates to
    ``_prepare_uprn_buffer`` to build the buffered catchment polygons.
    """
    if not isinstance(source, UprnSource):
        raise ValueError("Source is of incorrect type")

    if source.buffer_m is None:
        ogr_source = OgrSource(
            path=source.path,
            target_crs=source.crs,
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

    Dispatches on ``config.source`` type:

    - ``OgrSource`` — any GDAL-readable vector format (GeoPackage, Shapefile, …)
    - ``CsvSource`` — CSV with explicit x/y coordinate columns, or a WKT text column
    - ``ParquetSource`` — existing GeoParquet to re-sort and re-compress
    - ``UsrnSource``/``UprnSource`` — USRN/UPRN prep: plain (``buffer_m=None``)
      or buffered corridors/catchments (``buffer_m=<float>``)

    Output is always Hilbert-sorted, ZSTD-compressed GeoParquet 1.1 with bbox
    covering columns for SedonaDB row-group pruning.

    Parameters
    ----------
    config:
        Dataset configuration; ``source`` (required) drives dispatch.
    force:
        Re-prepare even if ``config.parquet_path`` already exists.
    threads:
        DuckDB thread count. ``None`` uses all available cores.
    memory_limit:
        DuckDB ``memory_limit`` for this call, e.g. ``"3GB"``. ``None`` leaves
        DuckDB's default of ~80% of system RAM, applied *per instance* — set
        this explicitly on a memory-constrained runner.

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
