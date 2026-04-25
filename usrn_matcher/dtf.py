"""DTF8.1-inspired export for USRN match results.

A community extension to the NSG DTF8.1 format for third-party spatially matched datasets.

Produces four output files per run — a DTF CSV, GeoParquet, flat CSV, and GeoPackage — all
carrying the matched RHS feature geometry (not the USRN street geometry).

See ``DTF_MAPPING.md`` at the repo root for the full compliance mapping and field layout.

Record order in the DTF CSV output:
    type 10  — Header                    (1 line, not counted in trailer)
    type 69  — ASD Metadata              (counted)
    type 63a — one per matched row       (USRN + attribution attributes only; counted)
    type 67a — one per matched row       (full WKT geometry of matched RHS feature; counted)
    type 99  — Trailer                   (record count = type69 + 63a + 67a)

Key deviations from strict DTF8.1 compliance
---------------------------------------------
- WHOLE_ROAD = 0 always (deliberate simplification — every match is treated
  as a part-road designation, never a whole-road one).
- ASD_COORDINATE = 1 always (because WHOLE_ROAD = 0; geometry always lives in
  the paired type 67a record — inline start/end coordinate fields are never
  emitted).
- For every matched row we ALWAYS emit a type 67a record carrying the full WKT
  geometry of the matched RHS feature. Standard type 67 omits a coordinate
  record for Point geometries (ASD_COORDINATE = 0 per spec footnote 52); we
  deviate and always write a type 67a so geometry handling is uniform across all
  OGC types.
- RECORD_IDENTIFIER = "63a" (non-standard; signals our extension of type 63).
  type 63a holds USRN attribution attributes only — no geometry fields.
- RECORD_IDENTIFIER = "67a" (non-standard; signals our extension of type 67).
  Geometry is stored as a single WKT string per feature rather than one row
  per vertex. ALL OGC simple feature types are supported: "PT", "L", "ML",
  "P", "MP" — including Point, which standard type 67 does not handle.
- RHS attribute columns appended after the fixed type 63a fields (index 12+).
- DTF_VERSION written as "8.1a" to signal the extension.

DTF8.1 data type rules respected
---------------------------------
- Text (T): enclosed in double quotes; empty value → ""
- Integer (I): no leading zeros; no thousands separators; empty → (nothing)
- Number (N): fixed decimal; no leading zeros; empty → (nothing)
- Date: ISO 8601 CCYY-MM-DD
- Process Time: HHMMSS

Usage:

    from usrn_matcher.dtf import DTFConfig, to_dtf_csv, to_dtf_geoparquet

    cfg = DTFConfig(swa_org_name="My Council", swa_org_ref=1234, rhs_name="stops")
    table = matcher.match_nearest(bbox=LEEDS, include_rhs_geometry=True)
    to_dtf_csv(table, cfg, "matched_data/matched_stops_ad.csv")
    to_dtf_geoparquet(table, cfg, "matched_data/matched_stops_ad.parquet")
"""

import logging
import pathlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import duckdb
import geopandas as gpd
import numpy as np
import pyarrow as pa
import pyarrow.csv as pcsv
import shapely
from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPolygon,
    Point,
    Polygon,
)

from .logger import get_logger
from .prepare import _BNG_BOX, _patch_covering_metadata

log: logging.Logger = get_logger()


@dataclass
class DTFConfig:
    """Configuration for DTF8.1-inspired export.

    Parameters
    ----------
    swa_org_name:
        Name of the organisation providing the data (T40). Written to the
        type 10 header as SWA_ORG_NAME_TEXT.
    swa_org_ref:
        Street Works Authority organisation reference code (I4). Use 0 if
        your organisation does not have a formal SWA code.
    rhs_name:
        Short name of the right-hand side dataset (e.g. ``"soil"``,
        ``"stops"``). Written as ATTRIBUTION_SOURCE_NAME in each type 63a
        record and as LINKED_DATA in the type 69 metadata record.
    """

    swa_org_name: str
    swa_org_ref: int
    rhs_name: str


# ---------------------------------------------------------------------------
# DTF8.1 field encoding helpers
# ---------------------------------------------------------------------------


def _enc_text(v: str | None, max_len: int) -> str:
    """Encode a text field: wrap in double quotes, truncate to max_len.

    Empty string or None → ``""`` (DTF spec 3.2.4).
    """
    if v is None or v == "":
        return '""'
    truncated: str = str(v)[:max_len]
    # Escape any double quotes inside the value per CSV convention
    escaped: str = truncated.replace('"', '""')
    return f'"{escaped}"'


def _enc_int(v: int | None) -> str:
    """Encode an integer field: no leading zeros.

    None → empty string (DTF spec 3.2.3 — two commas in context).
    """
    if v is None:
        return ""
    return str(int(v))


def _enc_num(v: float | None, decimals: int = 2) -> str:
    """Encode a numeric field: fixed decimal places, no leading zeros.

    None → empty string.
    """
    if v is None:
        return ""
    return f"{v:.{decimals}f}"


def _enc_date(d: date | None) -> str:
    """Encode a date as CCYY-MM-DD (ISO 8601).

    None → empty string.
    """
    if d is None:
        return ""
    return d.strftime("%Y-%m-%d")


def _enc_any(v: Any, field: pa.Field) -> str:
    """Auto-encode a value based on its Arrow field type."""
    if v is None:
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            return '""'
        return ""
    if pa.types.is_integer(field.type):
        return _enc_int(int(v))
    if pa.types.is_floating(field.type):
        return _enc_num(float(v))
    if pa.types.is_date(field.type):
        return _enc_date(v)
    # Everything else: treat as text
    return _enc_text(str(v), max_len=250)


# ---------------------------------------------------------------------------
# Geometry coordinate extraction
# ---------------------------------------------------------------------------

# Geometry type codes (extension to DTF8.1)
# ASD_GEOMETRY_TYPE codes for type 67a records — the extension covers all OGC simple features.
# Standard type 67 only defines "L" (line) and "P" (polygon); type 67a adds "PT", "ML", "MP".
_ASD_GEOM_TYPE: dict[str, str] = {
    "PT": "PT",
    "L": "L",
    "ML": "ML",
    "P": "P",
    "MP": "MP",
}


def _geom_type_code(geom: shapely.Geometry) -> str:
    """Return the DTF geometry type code for a shapely geometry.

    Codes: ``"PT"`` (Point), ``"L"`` (LineString), ``"ML"`` (MultiLineString),
    ``"P"`` (Polygon), ``"MP"`` (MultiPolygon).
    """
    match geom:
        case Point():
            return "PT"
        case LineString():
            return "L"
        case MultiLineString():
            return "ML"
        case Polygon():
            return "P"
        case MultiPolygon():
            return "MP"
        case _:
            log.warning(
                "Unsupported geometry type %r — defaulting to 'L'", type(geom).__name__
            )
            return "L"


# ---------------------------------------------------------------------------
# Record builder functions
# ---------------------------------------------------------------------------


def _type_10(config: DTFConfig, process_date: date, timestamp: str) -> str:
    """Build a type 10 Header record line.

    Example (from spec):
        10,"HALTON",0650,2008-06-26,1,2008-06-26,162500,"8.1.2.10","F"
    """
    fields = [
        "10",
        _enc_text(config.swa_org_name, 40),
        _enc_int(config.swa_org_ref),
        _enc_date(process_date),
        "1",  # VOLUME_NUMBER always 1
        _enc_date(process_date),  # ENTRY_DATE = PROCESS_DATE
        timestamp,  # TIME_STAMP HHMMSS (no quotes, numeric)
        '"8.1a"',  # DTF_VERSION — "8.1" part validated by NSG
        '"F"',  # FILE_TYPE = Full Supply
    ]
    return ",".join(fields)


def _type_69(config: DTFConfig, process_date: date) -> str:
    """Build a type 69 ASD Metadata record line.

    MD_* coverage percentage fields (24 fields, indices 14–37) are all set to 0.
    These fields measure what percentage of each standard NSG designation type
    (protected streets, speed limits, parking bays, etc.) is present in GeoPlace.
    They are a quality metric for standard ASD supply and have no meaningful value
    here — this file carries a third-party spatially matched dataset, not special
    designations. 0 satisfies the mandatory field requirement without claiming false
    coverage.
    """
    # 24 MD_* fields, all 0 (mandatory ones) or 0 (optional)
    md_zeros = ",".join(["0"] * 24)
    fields = [
        "69",
        _enc_text("England", 60),  # TER_OF_USE
        _enc_text(config.rhs_name, 100),  # LINKED_DATA
        '"M"',  # NGAZ_FREQ monthly
        _enc_text(config.swa_org_name, 40),  # CUSTODIAN_NAME
        "0",  # CUSTODIAN_UPRN (I12)
        _enc_int(config.swa_org_ref),  # AUTH_CODE (I4)
        '"British National Grid"',  # CO_ORD_SYSTEM
        '"Metres"',  # CO_ORD_UNIT
        _enc_date(process_date),  # META_DATE
        '"DTF8.1"',  # CLASS_SCHEME
        _enc_date(process_date),  # GAZ_DATE
        '"ENG"',  # LANGUAGE
        '"UTF-8"',  # CHARACTER_SET
        md_zeros,
    ]
    return ",".join(fields)


def _type_63a(
    pro_order: int,
    usrn: int,
    seq_num: int,
    geom_type_code: str,
    coord_count: int,
    rhs_attr_fields: list[str],
    config: DTFConfig,
    today: date,
) -> str:
    """Build a type 63a USRN Attribution Record line (DTF extension).

    This record holds attribution attributes only — geometry is always stored
    in the following type 67a coordinate record(s). ASD_COORDINATE is therefore
    always 1 (part-road designation with separate coordinate records).

    Field layout (CSV column indices, 0-based)
    ------------------------------------------
    Index  Field                    Type  Value
    -----  ─────────────────────────────────────────────────────────────────
    0      RECORD_IDENTIFIER        T3    "63a"  (non-standard extension)
    1      CHANGE_TYPE              T1    "I"
    2      PRO_ORDER                I16   sequential counter across all records
    3      USRN                     I8    street reference number
    4      ATTRIBUTION_SEQ_NUM      I3    sequential per USRN — counts how many RHS features have matched to this USRN (1, 2, 3 …)
    5      ATTRIBUTION_SOURCE_NAME  T120  config.rhs_name  e.g. "stops"
    6      WHOLE_ROAD               I1    0  (deliberate simplification — always part-road; whole-road not implemented)
    7      RECORD_START_DATE        Date  today
    8      LAST_UPDATE_DATE         Date  today
    9      ASD_COORDINATE           I1    1  (geometry always in type 67a)
    10     ASD_COORDINATE_COUNT     I3    1  (one type 67a per feature)
    11     GEOMETRY_TYPE            T2    "PT","L","ML","P","MP"
    -----  ─────────────────────────────────────────────────────────────────
    12+    *RHS attribute columns*        auto-encoded from the matched dataset
    -----  ─────────────────────────────────────────────────────────────────

    Note: SPECIAL_DESIG_START_X/Y (the spec's inline coordinate fields) are omitted —
    per DTF8.1 spec these are only required when ASD_COORDINATE=0. We always set
    ASD_COORDINATE=1 so geometry always lives in the paired type 67a record.
    """
    fields = [
        '"63a"',
        '"I"',
        _enc_int(pro_order),
        _enc_int(usrn),
        _enc_int(seq_num),
        _enc_text(config.rhs_name, 120),
        "0",  # WHOLE_ROAD — always part-road
        _enc_date(today),  # RECORD_START_DATE
        _enc_date(today),  # LAST_UPDATE_DATE
        "1",  # ASD_COORDINATE — always 1, geometry in type 67a
        _enc_int(coord_count),  # ASD_COORDINATE_COUNT
        _enc_text(geom_type_code, 2),  # GEOMETRY_TYPE
        *rhs_attr_fields,
    ]
    return ",".join(fields)


def _type_67a(
    pro_order: int,
    usrn: int,
    seq_num: int,
    asd_geom_type: str,
    geometry_wkt: str,
) -> str:
    """Build a type 67a ASD Coordinate Record line (our extension of type 67).

    Deviations from standard DTF8.1 type 67
    ----------------------------------------
    - RECORD_IDENTIFIER = "67a" (non-standard; signals our extension)
    - Geometry stored as a single WKT string rather than one row per vertex.
      Each matched feature produces exactly ONE type 67a record.
    - ASD_GEOMETRY_TYPE extended to all OGC simple feature types:
        "PT" (Point), "L" (LineString), "ML" (MultiLineString),
        "P" (Polygon), "MP" (MultiPolygon).
      Standard type 67 only defines "L" and "P".

    Field layout (CSV column indices, 0-based)
    ------------------------------------------
    Index  Field                Type  Value
    -----  ────────────────────────────────────────────────────────────────
    0      RECORD_IDENTIFIER    T3    "67a"  (non-standard extension)
    1      CHANGE_TYPE          T1    "I"
    2      PRO_ORDER            I16   sequential counter (follows parent 63a)
    3      ASD_GEOMETRY_TYPE    T2    "PT","L","ML","P","MP"
    4      ASD_RECORD_IDENTIFIER I2   63  (references parent type 63/63a)
    5      ASD_USRN             I8    street reference number
    6      ASD_SEQ_NUM          I3    matches ATTRIBUTION_SEQ_NUM in parent 63a
    7      GEOMETRY_WKT         Text  full OGC WKT string for the matched geometry
    -----  ────────────────────────────────────────────────────────────────

    Examples:
        "67a","I",2,"PT",63,5300050,1,"POINT (413064 429030)"
        "67a","I",4,"P",63,5300050,1,"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
        "67a","I",6,"ML",63,5300050,1,"MULTILINESTRING ((0 0, 1 1),(2 2, 3 3))"
    """
    fields = [
        '"67a"',
        '"I"',
        _enc_int(pro_order),
        _enc_text(asd_geom_type, 2),  # extended: "PT","L","ML","P","MP"
        "63",  # ASD_RECORD_IDENTIFIER (parent type 63/63a)
        _enc_int(usrn),  # ASD_USRN
        _enc_int(seq_num),  # ASD_SEQ_NUM
        _enc_text(geometry_wkt, 65535),  # GEOMETRY_WKT — full OGC WKT string
    ]
    return ",".join(fields)


def _type_99(record_count: int) -> str:
    """Build a type 99 Trailer record line."""
    return f"99,{record_count}"


# ---------------------------------------------------------------------------
# RHS attribute column extraction helpers
# ---------------------------------------------------------------------------

# Columns that belong to the fixed USRN join output — excluded from the
# trailing RHS attribute section of each type 63a record.
_SKIP_COLUMNS = frozenset({"usrn", "street_type", "geometry", "rhs_geometry"})


def _rhs_fields(schema: pa.Schema) -> list[tuple[int, pa.Field]]:
    """Return (column_index, field) pairs for RHS attribute columns.

    This is for faster lookups during the loop in creating the Dtf file.
    """
    return [
        (i, schema.field(i))
        for i in range(len(schema))
        if schema.field(i).name not in _SKIP_COLUMNS
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def to_dtf_csv(
    table: pa.Table,
    config: DTFConfig,
    path: str | pathlib.Path,
) -> pathlib.Path:
    """Write match results as a DTF8.1-inspired CSV.

    The ``table`` must contain a ``rhs_geometry`` column (WKB bytes) produced
    by calling :meth:`~usrn_matcher.UsrnMatcher.match_intersect` or
    :meth:`~usrn_matcher.UsrnMatcher.match_nearest` with
    ``include_rhs_geometry=True``.

    Parameters
    ----------
    table:
        Result table from a USRN spatial join.
    config:
        DTF export configuration (organisation name, SWA ref, RHS dataset name).
    path:
        Output file path (``*.csv``).

    Returns
    -------
    pathlib.Path
        Path to the written CSV file.

    Output structure
    ----------------
    One record per line, comma-separated, UTF-8 encoded:

    - **type 10**  — Header (1 line, not counted in trailer)
    - **type 69**  — ASD Metadata (1 line, counted)
    - Per result row:
      - **type 63a** — USRN + attribution attributes (counted)
      - **type 67a** — exactly one per matched feature, carrying the full WKT
        geometry of the RHS feature. All geometry types including Point always
        get a type 67a record (counted)
    - **type 99**  — Trailer (record count = type 69 + all 63a + all 67a)
    """
    resolved: pathlib.Path = pathlib.Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if "rhs_geometry" not in table.schema.names:
        raise ValueError(
            "Table is missing 'rhs_geometry' column. "
            "Call match_intersect() or match_nearest() with include_rhs_geometry=True."
        )

    today: date = date.today()
    now: datetime = datetime.now()
    timestamp: str = now.strftime("%H%M%S")

    # Convert columns to Python lists up front — one bulk conversion instead of
    # per-row .as_py() calls inside the loop.
    rhs_col_specs: list[tuple[int, pa.Field]] = _rhs_fields(table.schema)
    usrn_list: list[int] = table.column("usrn").to_pylist()
    rhs_geom_list: list[bytes | None] = table.column("rhs_geometry").to_pylist()
    rhs_col_lists: list[list] = [
        table.column(col_idx).to_pylist() for col_idx, _ in rhs_col_specs
    ]

    pro_order: int = 1  # sequential counter across all body records
    record_count: int = 0  # excludes header (10) and trailer (99)

    # Track per-USRN sequence numbers
    usrn_seq: dict[int, int] = {}

    lines: list[str] = []

    # Header + metadata written first (not counted in record_count for trailer)
    lines.append(_type_10(config, today, timestamp))
    lines.append(_type_69(config, today))
    record_count += 1  # type 69 counts as a body record

    for row_idx in range(len(table)):
        usrn: int = usrn_list[row_idx]
        rhs_wkb: bytes | None = rhs_geom_list[row_idx]

        if rhs_wkb is None:
            log.warning("Row %d has null rhs_geometry — skipping", row_idx)
            continue

        # Decode RHS geometry
        geom: shapely.Geometry = shapely.from_wkb(rhs_wkb)
        geom_type_code: str = _geom_type_code(geom)
        asd_geom_type: str = _ASD_GEOM_TYPE.get(geom_type_code, "L")
        geometry_wkt: str = shapely.to_wkt(geom)

        # Sequence number per USRN
        seq_num: int = usrn_seq.get(usrn, 0) + 1
        usrn_seq[usrn] = seq_num

        # Encode RHS attribute values
        rhs_attr_fields: list[str] = [
            _enc_any(rhs_col_lists[i][row_idx], field)
            for i, (_, field) in enumerate(rhs_col_specs)
        ]

        # Emit type 63a — attributes only; geometry follows in one type 67a record
        lines.append(
            _type_63a(
                pro_order=pro_order,
                usrn=usrn,
                seq_num=seq_num,
                geom_type_code=geom_type_code,
                coord_count=1,  # always 1 — one type 67a record per feature
                rhs_attr_fields=rhs_attr_fields,
                config=config,
                today=today,
            )
        )
        pro_order += 1
        record_count += 1

        # Emit type 67a — one record per feature carrying the full WKT geometry
        lines.append(
            _type_67a(
                pro_order=pro_order,
                usrn=usrn,
                seq_num=seq_num,
                asd_geom_type=asd_geom_type,
                geometry_wkt=geometry_wkt,
            )
        )
        pro_order += 1
        record_count += 1

    lines.append(_type_99(record_count))

    with open(resolved, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")

    log.info(
        "Written DTF CSV: %s (%d data records, %d total lines)",
        resolved,
        record_count,
        len(lines),
    )
    return resolved


def _build_dtf_gdf(table: pa.Table, config: DTFConfig) -> gpd.GeoDataFrame:
    """Build a spatially sorted GeoDataFrame with DTF type 63a column names.

    Shared by :func:`to_dtf_geoparquet` and :func:`to_dtf_flat_csv`. Column
    layout mirrors the type 63a CSV record — fixed DTF fields first, then RHS
    attribute columns, then the native geometry column (from rhs_geometry).

    The GeoDataFrame is sorted by Hilbert curve index (``ST_Hilbert`` via DuckDB
    in-memory) for spatial locality in the output GeoParquet.  DuckDB registers
    the source PyArrow table directly — no file I/O required.
    """

    today: date = date.today()
    n_rows: int = len(table)

    rhs_geom_col = table.column("rhs_geometry")
    geoms = shapely.from_wkb(rhs_geom_col.combine_chunks().to_pylist())
    geom_type_codes: list[str] = [_geom_type_code(g) for g in geoms]

    usrn_col = table.column("usrn").to_pylist()
    usrn_seq: dict[int, int] = {}
    seq_nums: list[int] = []
    for usrn in usrn_col:
        seq = usrn_seq.get(usrn, 0) + 1
        usrn_seq[usrn] = seq
        seq_nums.append(seq)

    dtf_cols: dict[str, list] = {
        "RECORD_IDENTIFIER": ["63a"] * n_rows,
        "CHANGE_TYPE": ["I"] * n_rows,
        "USRN": usrn_col,
        "ATTRIBUTION_SEQ_NUM": seq_nums,
        "ATTRIBUTION_SOURCE_NAME": [config.rhs_name] * n_rows,
        "WHOLE_ROAD": [0] * n_rows,
        "RECORD_START_DATE": [today] * n_rows,
        "LAST_UPDATE_DATE": [today] * n_rows,
        "ASD_COORDINATE": [1] * n_rows,
        "ASD_COORDINATE_COUNT": [1] * n_rows,
        "GEOMETRY_TYPE": geom_type_codes,
    }

    rhs_col_specs: list[tuple[int, pa.Field]] = _rhs_fields(table.schema)
    rhs_cols: dict[str, list] = {
        table.schema.field(col_idx).name: table.column(col_idx).to_pylist()
        for col_idx, _ in rhs_col_specs
    }

    gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
        {**dtf_cols, **rhs_cols},
        geometry=list(geoms),
        crs="EPSG:27700",
    )

    # Hilbert sort using DuckDB in-memory — register the original PyArrow table so
    # we can run ST_Hilbert on the rhs_geometry WKB column without touching disk.
    # COALESCE guards against any null rhs_geometry rows.
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("_dtf_src", table)
    hilbert_keys = np.asarray(
        con.sql(f"""
            SELECT COALESCE(
                ST_Hilbert(ST_GeomFromWKB(rhs_geometry), {_BNG_BOX}),
                0
            ) AS h
            FROM _dtf_src
        """).fetchnumpy()["h"],
        dtype=np.uint32,
    )

    idx = np.argsort(hilbert_keys)
    rows_moved = int((idx != np.arange(len(idx))).sum())
    log.debug(
        "Hilbert sort: %d rows, keys min=%d max=%d, %d/%d rows reordered",
        len(idx),
        int(hilbert_keys.min()),
        int(hilbert_keys.max()),
        rows_moved,
        len(idx),
    )
    return gpd.GeoDataFrame(gdf.iloc[idx].reset_index(drop=True))


def _write_dtf_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: pathlib.Path,
    row_group_size: int,
) -> None:
    """Write a Hilbert-sorted DTF GeoDataFrame as GeoParquet 1.1 via DuckDB.

    Converts the shapely geometry column to WKB, registers the resulting PyArrow
    table with DuckDB in-memory, and uses ``COPY TO PARQUET`` with an inline bbox
    struct column.  Follows the same pipeline as :func:`~usrn_matcher.prepare.prepare_dataset`:
    DuckDB writes the file, then :func:`~usrn_matcher.prepare._patch_covering_metadata`
    upgrades to GeoParquet 1.1 with the covering key and CRS PROJJSON.

    No temp file is written — avoids the GeoPandas two-pass round-trip.
    """

    # Convert shapely geometries → WKB bytes for DuckDB
    wkb = pa.array(shapely.to_wkb(gdf.geometry.values), type=pa.binary())

    # Build PyArrow table: non-geometry columns first, WKB geometry last
    base = pa.Table.from_pandas(gdf.drop(columns=["geometry"]), preserve_index=False)
    arrow_table = base.append_column(pa.field("geometry", pa.binary()), wkb)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("_dtf_write_src", arrow_table)

    # Inline bbox struct so DuckDB writes it alongside the geometry column.
    # A subquery materialises ST_GeomFromWKB once so the four ST_X*/ST_Y*
    # calls and the ST_Hilbert sort all reference the same geometry object.
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
                    * EXCLUDE geometry,
                    ST_GeomFromWKB(geometry) AS geometry
                FROM _dtf_write_src
            )
        ) TO '{path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size})
    """)

    _patch_covering_metadata(path, row_group_size, crs="EPSG:27700")


def to_dtf_geoparquet(
    table: pa.Table,
    config: DTFConfig,
    path: str | pathlib.Path,
    row_group_size: int = 10_000,
    *,
    _gdf: gpd.GeoDataFrame | None = None,
) -> pathlib.Path:
    """Write match results as a spatially optimised GeoParquet.

    Column layout mirrors the DTF type 63a record (see ``DTF_MAPPING.md``):
    fixed DTF fields, then RHS attribute columns, then a native geometry column
    (the matched RHS geometry — equivalent to the WKT in the paired type 67a).

    Uses the same DuckDB pipeline as the prepare phase: Hilbert sort, inline bbox
    struct, GeoParquet 1.1 covering key, ZSTD compression — no GeoPandas temp file.

    Parameters
    ----------
    table:
        Result table from a USRN spatial join (must contain ``rhs_geometry``).
    config:
        DTF export configuration — used to populate fixed DTF column values.
    path:
        Output file path (``*.parquet``).
    row_group_size:
        Row group size for the output GeoParquet (default 10,000).
    """
    resolved: pathlib.Path = pathlib.Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if "rhs_geometry" not in table.schema.names:
        raise ValueError(
            "Table is missing 'rhs_geometry' column. "
            "Call match_intersect() or match_nearest() with include_rhs_geometry=True."
        )

    gdf = _gdf if _gdf is not None else _build_dtf_gdf(table, config)
    _write_dtf_geoparquet(gdf, resolved, row_group_size)
    log.info("Written DTF GeoParquet: %s", resolved)
    return resolved


def to_dtf_flat_csv(
    table: pa.Table,
    config: DTFConfig,
    path: str | pathlib.Path,
    *,
    _gdf: gpd.GeoDataFrame | None = None,
) -> pathlib.Path:
    """Write match results as a spatially sorted flat CSV.

    One row per matched feature. Column layout is identical to the GeoParquet
    (DTF type 63a field names, then RHS attributes, then a ``geometry`` column
    containing the WKT geometry string). Rows are spatially sorted.

    Opens directly in QGIS via "Add Delimited Text Layer" (set geometry column
    to ``geometry``, CRS to EPSG:27700). Also readable in Excel, GeoPandas, etc.

    Parameters
    ----------
    table:
        Result table from a USRN spatial join (must contain ``rhs_geometry``).
    config:
        DTF export configuration — used to populate fixed DTF column values.
    path:
        Output file path (``*_flat.csv``).
    """
    resolved: pathlib.Path = pathlib.Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if "rhs_geometry" not in table.schema.names:
        raise ValueError(
            "Table is missing 'rhs_geometry' column. "
            "Call match_intersect() or match_nearest() with include_rhs_geometry=True."
        )

    gdf = _gdf if _gdf is not None else _build_dtf_gdf(table, config)

    base = pa.Table.from_pandas(gdf.drop(columns=["geometry"]), preserve_index=False)
    wkt_col = pa.array(shapely.to_wkt(gdf.geometry.values), type=pa.string())
    arrow_table = base.append_column(pa.field("geometry", pa.string()), wkt_col)
    pcsv.write_csv(arrow_table, str(resolved))

    log.info("Written DTF flat CSV: %s (%d rows)", resolved, len(gdf))
    return resolved


def to_dtf_gpkg(
    table: pa.Table,
    config: DTFConfig,
    path: str | pathlib.Path,
    layer: str | None = None,
    *,
    _gdf: gpd.GeoDataFrame | None = None,
) -> pathlib.Path:
    """Write match results as a GeoPackage layer.

    Column layout is identical to the GeoParquet and flat CSV (DTF type 63a
    field names, then RHS attributes, then native geometry). Rows are spatially
    sorted. Opens directly in QGIS, ArcGIS, and any OGR-compatible tool.

    Parameters
    ----------
    table:
        Result table from a USRN spatial join (must contain ``rhs_geometry``).
    config:
        DTF export configuration — used to populate fixed DTF column values.
    path:
        Output file path (``*.gpkg``).
    layer:
        Layer name inside the GeoPackage. Defaults to the file stem.
    """
    resolved: pathlib.Path = pathlib.Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if "rhs_geometry" not in table.schema.names:
        raise ValueError(
            "Table is missing 'rhs_geometry' column. "
            "Call match_intersect() or match_nearest() with include_rhs_geometry=True."
        )

    gdf = _gdf if _gdf is not None else _build_dtf_gdf(table, config)
    layer_name: str = layer or resolved.stem
    gdf.to_file(resolved, layer=layer_name, driver="GPKG")

    log.info(
        "Written DTF GeoPackage: %s (layer=%r, %d rows)", resolved, layer_name, len(gdf)
    )
    return resolved
