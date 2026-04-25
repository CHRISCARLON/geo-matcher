# DTF8.1 Field Mapping

This document describes how each record type produced by `usrn_matcher.dtf` maps to the NSG DTF8.1
specification — what is compliant, what differs, and why.

## Purpose and positioning

DTF8.1 type 63 and type 67 records were designed for highway authority ASD (Additional Street Data) exchange. 

Currently there is no built-in concept of spatially matching a third-party dataset to USRNs and attaching the results alongside the USRN record.

This file represents a **community extension to DTF8.1 for third-party spatially matched
datasets**. 

The intent is to carry matched RHS (right-hand side) feature geometry and attributes
alongside USRN records in a format that is as close to DTF8.1 as possible — so that existing
NSG-aware tooling can parse the fixed fields (record identifiers, PRO_ORDER, USRN, dates) without
modification, while treating the extension fields as pass-through data.

The core extension point is:

- **Type 70** — a single USRN attribution record that combines the attribution fixed fields with
  the matched dataset's attributes and the full OGC WKT geometry of the matched feature, all inline.
  One record per matched row; no paired coordinate record required.

`DTF_VERSION` is written as `"8.1a"` to signal to downstream consumers that this file uses these
extensions. 

---

## Type 10 — Header

**Status: compliant**

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | I2 | `10` |
| 1 | SWA_ORG_NAME_TEXT | T40 | `--dtf-org-name` argument |
| 2 | SWA_ORG_REF | I4 | `--dtf-org-ref` argument |
| 3 | PROCESS_DATE | Date | today |
| 4 | VOLUME_NUMBER | I2 | `1` |
| 5 | ENTRY_DATE | Date | today |
| 6 | TIME_STAMP | T6 | HHMMSS |
| 7 | DTF_VERSION | T10 | `"8.1a"` — `"8.1"` prefix is spec-valid; `"a"` signals our extension |
| 8 | FILE_TYPE | T1 | `"F"` (Full Supply) |

---

## Type 69 — ASD Metadata

**Status: compliant**

The 24 MD_* coverage percentage fields (indices 14–37) are all set to `0`.

They are a quality metric for the standard ASD supply and have no meaningful value here — this file carries a third-party spatially matched dataset, not special designations. 

`0` satisfies the mandatory field requirement without claiming false coverage; `100` would incorrectly imply complete NSG designation coverage.

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | I2 | `69` |
| 1 | TER_OF_USE | T60 | `"England"` |
| 2 | LINKED_DATA | T100 | RHS dataset name e.g. `"stops"`, `"soil"` |
| 3 | NGAZ_FREQ | T1 | `"M"` (monthly) |
| 4 | CUSTODIAN_NAME | T40 | `--dtf-org-name` argument |
| 5 | CUSTODIAN_UPRN | I12 | `0` |
| 6 | AUTH_CODE | I4 | `--dtf-org-ref` argument |
| 7 | CO_ORD_SYSTEM | T40 | `"British National Grid"` |
| 8 | CO_ORD_UNIT | T10 | `"Metres"` |
| 9 | META_DATE | Date | today |
| 10 | CLASS_SCHEME | T40 | `"DTF8.1"` |
| 11 | GAZ_DATE | Date | today |
| 12 | LANGUAGE | T3 | `"ENG"` |
| 13 | CHARACTER_SET | T30 | `"UTF-8"` |
| 14–37 | MD_* coverage % fields | I3 | `0` — not applicable, see note above |

---

## Type 70 — USRN Attribution Record (inline geometry)

**Status: extended** — `RECORD_IDENTIFIER = "70"` is not a valid DTF8.1 type.

Type 70 is a single record per matched row that combines the attribution fixed fields from the
standard type 63 (Special Designation) with the full OGC WKT geometry of the matched RHS feature
inline. There is no separate coordinate record — geometry is the last field.

**Design decisions (deliberate simplifications)**

- `WHOLE_ROAD = 0` always. Every match is treated as a part-road designation.
  Whole-road handling is not implemented. This is intentional — matched features
  from an RHS dataset always describe a specific location on or near a street,
  never the whole street (assumption for now).
- `ASD_COORDINATE` and `ASD_COORDINATE_COUNT` are **not written**. These fields in the standard
  type 63 indicate whether geometry is inline (0) or in a paired type 67 record (1). Type 70 has
  geometry inline as the last field, so the ASD_COORDINATE mechanism is redundant.
- `GEOMETRY_TYPE` is included to identify the OGC geometry type before the WKT field.

The standard type 63 (Special Designation) is the closest equivalent. 

Key differences:

| Difference | Standard type 63 | Our type 70 | Why |
|---|---|---|---|
| `RECORD_IDENTIFIER` | `63` (integer) | `"70"` (quoted string) | New type; avoids confusion with type 63 |
| Seq num field name | `STREET_SPECIAL_DESIG_NUM` (I3) | `ATTRIBUTION_SEQ_NUM` (I3) | Generic name — not specific to Special Designations |
| Type code field | `STREET_SPECIAL_DESIG_CODE` (I2 lookup) | `ATTRIBUTION_SOURCE_NAME` (T120 text) | We store the RHS dataset name, not a designation type code |
| `ASD_COORDINATE` / `ASD_COORDINATE_COUNT` | Present | **Omitted** | Type 70 carries geometry inline; the ASD_COORDINATE pairing mechanism is not used |
| Geometry storage | Inline start/end coords or paired type 67 record | Inline `GEOMETRY_WKT` as last field (T65535) | One record per feature; supports all OGC types including Point, MultiLineString, Polygon |
| `GEOMETRY_TYPE` | Not a standard field | Added at index 9 | Identifies OGC type before the WKT field |
| `DISTRICT_REF_CONSULTANT` | Conditional — operational district to consult | **Omitted** | Special designation workflow only; not applicable |
| `SOURCE_TEXT` | Optional T120 — free-text provenance note | **Omitted** | Covered by `ATTRIBUTION_SOURCE_NAME` (field 5) and `--dtf-org-name` in type 10 |
| Trailing RHS columns | Not in spec | Appended from index 10 onwards | Carries matched dataset attributes |
| `GEOMETRY_WKT` | Not in spec | Last field (index N) | Full OGC WKT geometry string for the matched RHS feature |

### Field layout

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | T2 | `"70"` |
| 1 | CHANGE_TYPE | T1 | `"I"` |
| 2 | PRO_ORDER | I16 | sequential counter across all body records |
| 3 | USRN | I8 | street reference number |
| 4 | ATTRIBUTION_SEQ_NUM | I3 | sequential per USRN (1, 2, 3 …) — counts how many RHS features have matched to this USRN |
| 5 | ATTRIBUTION_SOURCE_NAME | T120 | RHS dataset name e.g. `"stops"` |
| 6 | WHOLE_ROAD | I1 | `0` — always part-road |
| 7 | RECORD_START_DATE | Date | today |
| 8 | LAST_UPDATE_DATE | Date | today |
| 9 | GEOMETRY_TYPE | T2 | `"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"` |
| 10+ | *RHS attribute columns* | various | auto-encoded from matched dataset |
| N (last) | GEOMETRY_WKT | T65535 | full OGC WKT geometry string of the matched RHS feature |

### Example

Three bus stops matched to the same USRN — `ATTRIBUTION_SEQ_NUM` increments for each:

```
"70","I",1,5300050,1,"stops",0,2026-04-25,2026-04-25,"PT","490001234A","Acacia Avenue",...,"POINT (413064 429030)"
"70","I",2,5300050,2,"stops",0,2026-04-25,2026-04-25,"PT","490001235A","Acacia Avenue",...,"POINT (413080 429045)"
"70","I",3,5300050,3,"stops",0,2026-04-25,2026-04-25,"PT","490001236A","Acacia Avenue",...,"POINT (413100 429060)"
```

`ATTRIBUTION_SEQ_NUM` (field 4) is `1`, `2`, `3` — one per matched RHS feature on this USRN.
Fields 0–9 are the DTF-derived fixed fields. From field 10 onwards are the matched dataset
attributes (NaPTAN columns in this case), with `GEOMETRY_WKT` as the final field.

---

## Type 99 — Trailer

**Status: compliant**

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | I2 | `99` |
| 1 | RECORD_COUNT | I8 | count of body records (type 69 + all type 70) |

Type 10 (header) and type 99 (trailer) are excluded from the count.

---

## PRO_ORDER

`PRO_ORDER` is a mandatory DTF8.1 field — a **global sequential integer** assigned to every body
record in the file. It lets processors detect gaps, duplicates, or truncated files.

- Starts at `1` for the first record after the type 69 header.
- Increments by 1 for every type 70 written.
- Type 10 (header) and type 99 (trailer) are **not** assigned a PRO_ORDER.
- Type 69 is written before the loop and is not assigned a PRO_ORDER either; it is however
  included in `RECORD_COUNT` in the trailer.

```
69,...                         ← no PRO_ORDER
"70","I",1,...                 ← PRO_ORDER = 1
"70","I",2,...                 ← PRO_ORDER = 2
"70","I",3,...                 ← PRO_ORDER = 3
99,{N}                         ← N = 1 (type 69) + number of type 70 rows
```

---

## File structure summary

```
10,...                                            ← header (1 line, not counted)
69,...                                            ← ASD metadata (counted, no PRO_ORDER)
"70","I",1,...,"POINT (413064 429030)"            ← match 1 (PRO_ORDER=1, counted)
"70","I",2,...,"POINT (413080 429045)"            ← match 2 (PRO_ORDER=2, counted)
"70","I",3,...,"POLYGON ((0 0, 1 0, 1 1, 0 0))"  ← match 3 (PRO_ORDER=3, counted)
...
99,{N}                                            ← N = 1 (type 69) + number of matched rows
```

---

## Output files

A single `usrn-matcher dtf-export` run produces four files in `matched_data/`:

| File | Format | Description |
|---|---|---|
| `matched_{name}_ad.csv` | DTF 8.1a | Type 70 records. Exchange format for NSG-aware tools. |
| `matched_{name}_ad.parquet` | GeoParquet 1.1 | Spatially optimised. One row per feature. DTF column names. Bbox covering + ZSTD + spatial sort. |
| `matched_{name}_ad_flat.csv` | Flat CSV | Same column structure as the parquet, WKT geometry column. Opens in QGIS (Add Delimited Text Layer), Excel, GeoPandas. |
| `matched_{name}_ad.gpkg` | GeoPackage | Same column structure as the parquet, native geometry. Opens in QGIS, ArcGIS, any OGR-compatible tool. |

The parquet, flat CSV and GeoPackage all share the same structure — DTF type 70 fixed field names
(indices 0–9), then the RHS dataset attribute columns, then the geometry. They are all spatially
sorted by the RHS geometry centroid using a Hilbert curve.

---

## CSV deviations from DTF8.1 — summary

For readers comparing the output against the NSG DTF8.1 specification:

| Area | DTF8.1 spec | This file | Reason |
|---|---|---|---|
| `RECORD_IDENTIFIER` for attribution | `63` (integer) | `"70"` (quoted string) | New combined type; avoids confusion with standard type 63/67 |
| Geometry storage | Paired type 67 record per feature (one row per vertex) | Inline `GEOMETRY_WKT` as last field of type 70 (full OGC WKT string) | One record per feature regardless of vertex count; supports all OGC types |
| `ASD_COORDINATE` / `ASD_COORDINATE_COUNT` | Present in type 63 | **Omitted** | Type 70 carries geometry inline; the ASD_COORDINATE pairing mechanism is not used |
| `WHOLE_ROAD` | `0` or `1` | Always `0` | Deliberate simplification — all matches treated as part-road; whole-road is not implemented |
| Point geometry storage | Spec footnote 52: Points use `ASD_COORDINATE=0`; inline `SPECIAL_DESIG_START_X/Y` | WKT `POINT (x y)` as last field of type 70 | Uniform path for all geometry types |
| `GEOMETRY_TYPE` | Not a standard field | Added at index 9, before RHS attrs | Identifies OGC type (`"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"`) before the WKT field |
| RHS attribute columns | Not in spec (type 63 has fixed fields only) | Appended from index 10 onwards | Carries the matched dataset attributes alongside the USRN record |
| `GEOMETRY_WKT` | Not in spec | Last field of each type 70 record | Full OGC WKT geometry string for the matched RHS feature |
| `DTF_VERSION` | `"8.1"` | `"8.1a"` | `"8.1"` prefix is spec-valid; `"a"` suffix signals our extension |
