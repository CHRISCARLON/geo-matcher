"""DTF8.1-inspired export for USRN match results.

A community extension to the NSG DTF8.1 format for third-party spatially matched datasets.

Produces four output files per run — a DTF CSV, GeoParquet, flat CSV, and GeoPackage — all
carrying the matched RHS feature geometry (not the USRN street geometry).

See ``docs/dtf-mapping.md`` at the repo root for the full compliance mapping and field layout.

Record order in the DTF CSV output:
    type 10  — Header                    (1 line, not counted in trailer)
    type 69  — ASD Metadata              (counted)
    type 70  — one per matched row       (USRN + attribution attributes + inline WKT geometry; counted)
    type 99  — Trailer                   (record count = type69 + type70 rows)

Key deviations from strict DTF8.1 compliance
---------------------------------------------
- RECORD_IDENTIFIER = "70" (non-standard; our extension combining type 63 attribution
  with inline RHS geometry — replaces the paired type 63a + type 67a approach).
- WHOLE_ROAD = 0 always (deliberate simplification — every match is treated
  as a part-road designation, never a whole-road one).
- GEOMETRY_TYPE extended to all OGC simple feature types: "PT", "L", "ML", "P", "MP".
  Standard DTF8.1 type 67 only defines "L" (line) and "P" (polygon).
- GEOMETRY_WKT stored as a single WKT string per feature (last field of each type 70 record).
- RHS attribute columns appended after the fixed type 70 fields (index 10+), before GEOMETRY_WKT.
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
    table = matcher.match_dispatch("nearest", bbox=LEEDS, include_rhs_geometry=True)
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
        ``"stops"``). Written as ATTRIBUTION_SOURCE_NAME in each type 70
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
    return _enc_text(str(v), max_len=250)


# ---------------------------------------------------------------------------
# Geometry coordinate extraction
# ---------------------------------------------------------------------------

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
    """Build a type 10 Header record line."""
    fields = [
        "10",
        _enc_text(config.swa_org_name, 40),
        _enc_int(config.swa_org_ref),
        _enc_date(process_date),
        "1",
        _enc_date(process_date),
        timestamp,
        '"8.1a"',
        '"F"',
    ]
    return ",".join(fields)


def _type_69(config: DTFConfig, process_date: date) -> str:
    """Build a type 69 ASD Metadata record line."""
    md_zeros = ",".join(["0"] * 24)
    fields = [
        "69",
        _enc_text("England", 60),
        _enc_text(config.rhs_name, 100),
        '"M"',
        _enc_text(config.swa_org_name, 40),
        "0",
        _enc_int(config.swa_org_ref),
        '"British National Grid"',
        '"Metres"',
        _enc_date(process_date),
        '"DTF8.1"',
        _enc_date(process_date),
        '"ENG"',
        '"UTF-8"',
        md_zeros,
    ]
    return ",".join(fields)


def _type_70(
    pro_order: int,
    usrn: int,
    seq_num: int,
    geom_type_code: str,
    rhs_attr_fields: list[str],
    geometry_wkt: str,
    config: DTFConfig,
    today: date,
) -> str:
    """Build a type 70 USRN Attribution + Geometry record line (DTF extension).

    Combines the attribution fields of the former type 63a with the inline WKT
    geometry of the former type 67a into a single record, removing the need for
    paired records.

    Field layout (CSV column indices, 0-based)
    ------------------------------------------
    Index  Field                    Type  Value
    -----  ─────────────────────────────────────────────────────────────────
    0      RECORD_IDENTIFIER        T3    "70"  (non-standard extension)
    1      CHANGE_TYPE              T1    "I"
    2      PRO_ORDER                I16   sequential counter across all records
    3      USRN                     I8    street reference number
    4      ATTRIBUTION_SEQ_NUM      I3    sequential per USRN
    5      ATTRIBUTION_SOURCE_NAME  T120  config.rhs_name
    6      WHOLE_ROAD               I1    0  (always part-road)
    7      RECORD_START_DATE        Date  today
    8      LAST_UPDATE_DATE         Date  today
    9      GEOMETRY_TYPE            T2    "PT","L","ML","P","MP"
    -----  ─────────────────────────────────────────────────────────────────
    10+    *RHS attribute columns*        auto-encoded from the matched dataset
    -----  ─────────────────────────────────────────────────────────────────
    N      GEOMETRY_WKT             T     full OGC WKT string of matched RHS feature
    -----  ─────────────────────────────────────────────────────────────────
    """
    fields = [
        '"70"',
        '"I"',
        _enc_int(pro_order),
        _enc_int(usrn),
        _enc_int(seq_num),
        _enc_text(config.rhs_name, 120),
        "0",
        _enc_date(today),
        _enc_date(today),
        _enc_text(geom_type_code, 2),
        *rhs_attr_fields,
        _enc_text(geometry_wkt, 65535),
    ]
    return ",".join(fields)


def _type_99(record_count: int) -> str:
    """Build a type 99 Trailer record line."""
    return f"99,{record_count}"


# ---------------------------------------------------------------------------
# RHS attribute column extraction helpers
# ---------------------------------------------------------------------------

_SKIP_COLUMNS = frozenset({"usrn", "street_type", "geometry", "rhs_geometry"})


def _rhs_fields(schema: pa.Schema) -> list[tuple[int, pa.Field]]:
    """Return (column_index, field) pairs for RHS attribute columns."""
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
    by calling :meth:`~usrn_matcher.UsrnMatcher.match_dispatch` with ``include_rhs_geometry=True``.

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
      - **type 70** — USRN attribution + inline RHS geometry WKT (counted)
    - **type 99**  — Trailer (record count = type 69 + all type 70 rows)
    """
    resolved: pathlib.Path = pathlib.Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if "rhs_geometry" not in table.schema.names:
        raise ValueError(
            "Table is missing 'rhs_geometry' column. "
            "Call match_dispatch() with include_rhs_geometry=True."
        )

    today: date = date.today()
    now: datetime = datetime.now()
    timestamp: str = now.strftime("%H%M%S")

    rhs_col_specs: list[tuple[int, pa.Field]] = _rhs_fields(table.schema)
    usrn_list: list[int] = table.column("usrn").to_pylist()
    rhs_geom_list: list[bytes | None] = table.column("rhs_geometry").to_pylist()
    rhs_col_lists: list[list] = [
        table.column(col_idx).to_pylist() for col_idx, _ in rhs_col_specs
    ]

    pro_order: int = 1
    record_count: int = 0
    usrn_seq: dict[int, int] = {}
    lines: list[str] = []

    lines.append(_type_10(config, today, timestamp))
    lines.append(_type_69(config, today))
    record_count += 1  # type 69

    for row_idx in range(len(table)):
        usrn: int = usrn_list[row_idx]
        rhs_wkb: bytes | None = rhs_geom_list[row_idx]

        if rhs_wkb is None:
            log.warning("Row %d has null rhs_geometry — skipping", row_idx)
            continue

        geom: shapely.Geometry = shapely.from_wkb(rhs_wkb)
        geom_type_code: str = _geom_type_code(geom)
        geometry_wkt: str = shapely.to_wkt(geom)

        seq_num: int = usrn_seq.get(usrn, 0) + 1
        usrn_seq[usrn] = seq_num

        rhs_attr_fields: list[str] = [
            _enc_any(rhs_col_lists[i][row_idx], field)
            for i, (_, field) in enumerate(rhs_col_specs)
        ]

        lines.append(
            _type_70(
                pro_order=pro_order,
                usrn=usrn,
                seq_num=seq_num,
                geom_type_code=geom_type_code,
                rhs_attr_fields=rhs_attr_fields,
                geometry_wkt=geometry_wkt,
                config=config,
                today=today,
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


def _build_dtf_table(table: pa.Table, config: DTFConfig) -> pa.Table:
    """Build a Hilbert-sorted PyArrow table with DTF type 70 column names.

    Shared by :func:`to_dtf_geoparquet`, :func:`to_dtf_flat_csv`, and
    :func:`to_dtf_gpkg`. Column layout: fixed DTF fields, then RHS attribute
    columns, then a ``geometry`` column (WKB binary — the matched RHS geometry).
    Rows are sorted by Hilbert curve index for spatial locality.
    """
    today: date = date.today()
    n_rows: int = len(table)

    rhs_wkb: pa.ChunkedArray = table.column("rhs_geometry")
    rhs_wkb_flat: pa.Array = rhs_wkb.combine_chunks()
    # binary_view (e.g. from DuckDB ST_AsWKB) doesn't support take(); cast to plain binary
    if "view" in str(rhs_wkb_flat.type):
        rhs_wkb_flat = rhs_wkb_flat.cast(pa.binary())
    geoms = shapely.from_wkb(rhs_wkb_flat.to_pylist())
    geom_type_codes: list[str] = [_geom_type_code(g) for g in geoms]

    usrn_list: list[int] = table.column("usrn").to_pylist()
    usrn_seq: dict[int, int] = {}
    seq_nums: list[int] = []
    for usrn in usrn_list:
        s: int = usrn_seq.get(usrn, 0) + 1
        usrn_seq[usrn] = s
        seq_nums.append(s)

    dtf_arrays: dict[str, pa.Array | pa.ChunkedArray] = {
        "RECORD_IDENTIFIER": pa.array(["70"] * n_rows),
        "CHANGE_TYPE": pa.array(["I"] * n_rows),
        "USRN": table.column("usrn"),
        "ATTRIBUTION_SEQ_NUM": pa.array(seq_nums, type=pa.int32()),
        "ATTRIBUTION_SOURCE_NAME": pa.array([config.rhs_name] * n_rows),
        "WHOLE_ROAD": pa.array([0] * n_rows, type=pa.int8()),
        "RECORD_START_DATE": pa.array([today] * n_rows),
        "LAST_UPDATE_DATE": pa.array([today] * n_rows),
        "GEOMETRY_TYPE": pa.array(geom_type_codes),
    }

    rhs_col_specs: list[tuple[int, pa.Field]] = _rhs_fields(table.schema)
    rhs_arrays: dict[str, pa.Array | pa.ChunkedArray] = {}
    for col_idx, _ in rhs_col_specs:
        name = table.schema.field(col_idx).name
        col = table.column(col_idx)
        # *_view types don't support take(); downcast to the non-view equivalent
        t = str(col.type)
        if "view" in t or pa.types.is_large_string(col.type):
            col = col.cast(pa.binary() if "binary" in t else pa.string())
        rhs_arrays[name] = col

    dtf_tbl: pa.Table = pa.table({**dtf_arrays, **rhs_arrays, "geometry": rhs_wkb_flat})

    # Hilbert sort via DuckDB — registers original table (has rhs_geometry for ST_Hilbert)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("_dtf_src", table)
    hilbert_keys: np.ndarray = np.asarray(
        con.sql(f"""
            SELECT COALESCE(ST_Hilbert(ST_GeomFromWKB(rhs_geometry), {_BNG_BOX}), 0) AS h
            FROM _dtf_src
        """).fetchnumpy()["h"],
        dtype=np.uint32,
    )
    return dtf_tbl.take(np.argsort(hilbert_keys))


def _write_dtf_geoparquet(
    dtf_table: pa.Table,
    path: pathlib.Path,
    row_group_size: int,
) -> None:
    """Write a Hilbert-sorted DTF Arrow table as GeoParquet 1.1 via DuckDB.

    The ``geometry`` column must be WKB binary. DuckDB converts it to a native
    geometry type, writes the file with an inline bbox struct column, then
    :func:`~usrn_matcher.prepare._patch_covering_metadata` upgrades to GeoParquet
    1.1 with the covering key and CRS PROJJSON.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("_dtf_write_src", dtf_table)
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
    _dtf_table: pa.Table | None = None,
) -> pathlib.Path:
    """Write match results as a spatially optimised GeoParquet.

    Column layout mirrors the DTF type 70 record: fixed DTF fields, then RHS
    attribute columns, then a native geometry column (the matched RHS geometry).

    Parameters
    ----------
    table:
        Result table from a USRN spatial join (must contain ``rhs_geometry``).
    config:
        DTF export configuration.
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
            "Call match_dispatch() with include_rhs_geometry=True."
        )

    dtf_tbl = _dtf_table if _dtf_table is not None else _build_dtf_table(table, config)
    _write_dtf_geoparquet(dtf_tbl, resolved, row_group_size)
    log.info("Written DTF GeoParquet: %s", resolved)
    return resolved


def to_dtf_flat_csv(
    table: pa.Table,
    config: DTFConfig,
    path: str | pathlib.Path,
    *,
    _dtf_table: pa.Table | None = None,
) -> pathlib.Path:
    """Write match results as a spatially sorted flat CSV.

    One row per matched feature. Column layout is identical to the GeoParquet
    (DTF type 70 field names, then RHS attributes, then a ``geometry`` column
    containing the WKT geometry string). Rows are spatially sorted.

    Parameters
    ----------
    table:
        Result table from a USRN spatial join (must contain ``rhs_geometry``).
    config:
        DTF export configuration.
    path:
        Output file path (``*_flat.csv``).
    """
    resolved: pathlib.Path = pathlib.Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)

    if "rhs_geometry" not in table.schema.names:
        raise ValueError(
            "Table is missing 'rhs_geometry' column. "
            "Call match_dispatch() with include_rhs_geometry=True."
        )

    dtf_tbl = _dtf_table if _dtf_table is not None else _build_dtf_table(table, config)

    geoms = shapely.from_wkb(dtf_tbl.column("geometry").to_pylist())
    wkt_col: pa.Array = pa.array(shapely.to_wkt(geoms), type=pa.string())
    geom_idx: int = dtf_tbl.schema.get_field_index("geometry")
    out_table: pa.Table = dtf_tbl.set_column(geom_idx, "geometry", wkt_col)
    pcsv.write_csv(out_table, str(resolved))

    log.info("Written DTF flat CSV: %s (%d rows)", resolved, len(dtf_tbl))
    return resolved


def to_dtf_gpkg(
    table: pa.Table,
    config: DTFConfig,
    path: str | pathlib.Path,
    layer: str | None = None,
    *,
    _dtf_table: pa.Table | None = None,
) -> pathlib.Path:
    """Write match results as a GeoPackage layer.

    Column layout is identical to the GeoParquet and flat CSV (DTF type 70
    field names, then RHS attributes, then native geometry). Rows are spatially
    sorted. Opens directly in QGIS, ArcGIS, and any OGR-compatible tool.

    Parameters
    ----------
    table:
        Result table from a USRN spatial join (must contain ``rhs_geometry``).
    config:
        DTF export configuration.
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
            "Call match_dispatch() with include_rhs_geometry=True."
        )

    dtf_tbl = _dtf_table if _dtf_table is not None else _build_dtf_table(table, config)
    layer_name: str = layer or resolved.stem

    geoms = shapely.from_wkb(dtf_tbl.column("geometry").to_pylist())
    non_geom: pa.Table = dtf_tbl.drop(["geometry"])
    gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
        non_geom.to_pandas(), geometry=list(geoms), crs="EPSG:27700"
    )
    gdf.to_file(resolved, layer=layer_name, driver="GPKG")

    log.info(
        "Written DTF GeoPackage: %s (layer=%r, %d rows)",
        resolved,
        layer_name,
        len(dtf_tbl),
    )
    return resolved
