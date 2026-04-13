# DTF8.1 Field Mapping

This document describes how each record type produced by `usrn_matcher.dtf` maps to the NSG DTF8.1
specification — what is compliant, what differs, and why.

## Purpose and positioning

DTF8.1 was designed for highway authority ASD (Additional Street Data) exchange. 

It has no built-in concept of spatially matching a third-party dataset to USRNs and attaching the results alongside the USRN record.

This file represents a **community extension to DTF8.1 for third-party spatially matched
datasets**. 

The intent is to carry matched RHS (right-hand side) feature geometry and attributes
alongside USRN records in a format that is as close to DTF8.1 as possible — so that existing
NSG-aware tooling can parse the fixed fields (record identifiers, PRO_ORDER, USRN, dates) without
modification, while treating the extension fields as pass-through data.

The core extension points are:

- **Type 63a** — a USRN attribution record that replaces the Special Designation type code with
  a defined source name and appends the matched dataset's attributes as trailing columns.
- **Type 67a** — a coordinate record that stores the full OGC WKT geometry of the matched feature
  in a single record.

`DTF_VERSION` is written as `"8.1a"` to signal to downstream consumers that this file uses these
extensions. 

---

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

The 24 MD_* coverage percentage fields (indices 14–37) are all set to `0`. These fields measure
what percentage of each standard NSG designation type is present in GeoPlace for the supplying authority. 

They are a quality metric for the standard ASD supply and have no meaningful value here — this file carries a third-party spatially matched dataset, not special designations. `0` satisfies the mandatory field requirement without claiming false coverage; `100` would incorrectly imply complete NSG designation coverage.

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

## Type 63a — USRN Attribution Record

**Status: extended** — `RECORD_IDENTIFIER = "63a"` is not a valid DTF8.1 type.

**Design decisions (deliberate simplifications)**

- `WHOLE_ROAD = 0` always. Every match is treated as a part-road designation.
  Whole-road handling is not implemented. This is intentional — matched features
  from an RHS dataset always describe a specific location on or near a street,
  never the whole street.
- Because `WHOLE_ROAD = 0`, the spec requires `ASD_COORDINATE` to be set.
  We always set `ASD_COORDINATE = 1`, meaning geometry is always carried in a
  paired type 67a record. Inline start/end coordinate fields are never written.
- A type 67a record is **always** emitted for every matched row, carrying the
  full WKT geometry of the matched RHS feature. This applies to all geometry
  types including Point. (The standard DTF8.1 spec, footnote 52, says Points
  should use `ASD_COORDINATE = 0` with no type 67 record — we deviate here for
  uniformity.)

The standard type 63 (Special Designation) is the closest equivalent. 

Key differences:

| Difference | Standard type 63 | Our type 63a | Why |
|---|---|---|---|
| `RECORD_IDENTIFIER` | `63` (integer) | `"63a"` (quoted string) | Signals our extension |
| Seq num field name | `STREET_SPECIAL_DESIG_NUM` (I3) | `ATTRIBUTION_SEQ_NUM` (I3) | Generic name — not specific to Special Designations |
| Type code field | `STREET_SPECIAL_DESIG_CODE` (I2 lookup) | `ATTRIBUTION_SOURCE_NAME` (T120 text) | We store the RHS dataset name, not a designation type code |
| `SPECIAL_DESIG_START_X/Y` | Present when `ASD_COORDINATE=0` | **Omitted** | We always use `ASD_COORDINATE=1`; inline coords never needed |
| Point geometry | Spec footnote 52: Points must use `ASD_COORDINATE=0`; no type 67 record | We set `ASD_COORDINATE=1` for Points; geometry always in type 67a | Uniform path for all geometry types |
| `GEOMETRY_TYPE` | Not a standard field | Added at index 11 | Needed to identify OGC type in paired type 67a |
| `DISTRICT_REF_CONSULTANT` | Conditional — operational district to consult | **Omitted** | Special designation workflow only; not applicable |
| `SOURCE_TEXT` | Optional T120 — free-text provenance note | **Omitted** | Covered by `ATTRIBUTION_SOURCE_NAME` (field 5) and `--dtf-org-name` in type 10; could be used for a richer provenance string (e.g. `"NaPTAN extract 2026-04-13 from DfT"`) if needed |
| Trailing RHS columns | Not in spec | Appended from index 12 onwards | Carries matched dataset attributes |

### Field layout

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | T3 | `"63a"` |
| 1 | CHANGE_TYPE | T1 | `"I"` |
| 2 | PRO_ORDER | I16 | sequential counter across all body records |
| 3 | USRN | I8 | street reference number |
| 4 | ATTRIBUTION_SEQ_NUM | I3 | sequential per USRN (1, 2, 3 …) — counts how many RHS features have matched to this USRN |
| 5 | ATTRIBUTION_SOURCE_NAME | T120 | RHS dataset name e.g. `"stops"` |
| 6 | WHOLE_ROAD | I1 | `0` — always part-road |
| 7 | RECORD_START_DATE | Date | today |
| 8 | LAST_UPDATE_DATE | Date | today |
| 9 | ASD_COORDINATE | I1 | `1` — geometry always in paired type 67a |
| 10 | ASD_COORDINATE_COUNT | I3 | `1` — one type 67a per feature |
| 11 | GEOMETRY_TYPE | T2 | `"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"` |
| 12+ | *RHS attribute columns* | various | auto-encoded from matched dataset |

### Example

Three bus stops matched to the same USRN — `ATTRIBUTION_SEQ_NUM` increments for each:

```
"63a","I",1,5300050,1,"stops",0,2026-04-13,2026-04-13,1,1,"PT","490001234A","Acacia Avenue",...
"67a","I",2,"PT",63,5300050,1,"POINT (413064 429030)"
"63a","I",3,5300050,2,"stops",0,2026-04-13,2026-04-13,1,1,"PT","490001235A","Acacia Avenue",...
"67a","I",4,"PT",63,5300050,2,"POINT (413080 429045)"
"63a","I",5,5300050,3,"stops",0,2026-04-13,2026-04-13,1,1,"PT","490001236A","Acacia Avenue",...
"67a","I",6,"PT",63,5300050,3,"POINT (413100 429060)"
```

`ATTRIBUTION_SEQ_NUM` (field 4) is `1`, `2`, `3` — one per matched RHS feature on this USRN.
Fields 0–11 are the DTF-derived fixed fields. From field 12 onwards are the matched dataset
attributes (NaPTAN columns in this case).

---

## Type 67a — ASD Coordinate Record

**Status: extended** — `RECORD_IDENTIFIER = "67a"` is not a valid DTF8.1 type.

Every type 63a record is **always** followed by exactly one type 67a record. The type 67a carries
the full WKT geometry of the matched RHS feature — regardless of geometry type, including Points.

The standard type 67 (ASD Coordinate Record) uses individual rows for each X/Y vertex. Our type
67a replaces all vertex rows with a single WKT string per feature.

| Difference | Standard type 67 | Our type 67a | Why |
|---|---|---|---|
| `RECORD_IDENTIFIER` | `67` (integer) | `"67a"` (quoted string) | Signals our extension |
| `ASD_GEOMETRY_TYPE` | `"L"` or `"P"` only | `"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"` | Support all OGC simple features including Point |
| Geometry storage | One row per vertex (`ASD_X_COORDINATE`, `ASD_Y_COORDINATE`, `COORD_NUMBER`) | One row per feature (`GEOMETRY_WKT`) | Simpler — one record per matched feature regardless of vertex count |
| Point geometries | Not stored in type 67 (inline in type 63) | Stored as `POINT (x y)` WKT | Uniform path for all geometry types |

### Field layout

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | T3 | `"67a"` |
| 1 | CHANGE_TYPE | T1 | `"I"` |
| 2 | PRO_ORDER | I16 | sequential (immediately follows parent type 63a) |
| 3 | ASD_GEOMETRY_TYPE | T2 | `"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"` |
| 4 | ASD_RECORD_IDENTIFIER | I2 | `63` — references parent type 63/63a |
| 5 | ASD_USRN | I8 | matches USRN in parent type 63a |
| 6 | ASD_SEQ_NUM | I3 | matches ATTRIBUTION_SEQ_NUM in parent type 63a |
| 7 | GEOMETRY_WKT | Text | full OGC WKT geometry string |

### Examples

```
"67a","I",2,"PT",63,5300050,1,"POINT (413064 429030)"
"67a","I",4,"P",63,5300050,1,"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
"67a","I",6,"ML",63,5300050,1,"MULTILINESTRING ((0 0, 1 1),(2 2, 3 3))"
```

---

## Type 99 — Trailer

**Status: compliant**

| Index | Field | Type | Value |
|---|---|---|---|
| 0 | RECORD_IDENTIFIER | I2 | `99` |
| 1 | RECORD_COUNT | I8 | count of body records (type 69 + all 63a + all 67a) |

Type 10 (header) and type 99 (trailer) are excluded from the count.

---

## PRO_ORDER

`PRO_ORDER` is a mandatory DTF8.1 field — a **global sequential integer** assigned to every body
record in the file. It lets processors detect gaps, duplicates, or truncated files.

- Starts at `1` for the first record after the type 69 header.
- Increments by 1 for every type 63a and type 67a written.
- Type 10 (header) and type 99 (trailer) are **not** assigned a PRO_ORDER.
- Type 69 is written before the loop and is not assigned a PRO_ORDER either; it is however
  included in `RECORD_COUNT` in the trailer.

```
69,...                         ← no PRO_ORDER
"63a","I",1,...                ← PRO_ORDER = 1
"67a","I",2,...                ← PRO_ORDER = 2
"63a","I",3,...                ← PRO_ORDER = 3
"67a","I",4,...                ← PRO_ORDER = 4
99,{N}                         ← N = pro_order - 1 + 1 (type 69)
```

---

## 63a ↔ 67a linkage

Each type 63a is **immediately followed** by exactly one type 67a. They are linked three ways:

| Method | Fields | Notes |
|---|---|---|
| **Adjacency** | PRO_ORDER is consecutive: 63a = N, 67a = N+1 | Always true in this file — implicit |
| **USRN + seq** | `USRN` / `ATTRIBUTION_SEQ_NUM` in 63a = `ASD_USRN` / `ASD_SEQ_NUM` in 67a | Explicit join key — survives reordering |
| **Parent type** | `ASD_RECORD_IDENTIFIER = 63` in 67a | Identifies the parent record type (63/63a) |

```
"63a","I", 1, 5300050, 1, ...
"67a","I", 2, "PT", 63, 5300050, 1, "POINT (413064 429030)"
                        ↑         ↑
                    ASD_USRN   ASD_SEQ_NUM  — join back to parent 63a
```

`(ASD_USRN, ASD_SEQ_NUM)` is the reliable join key if the file is ever split into separate
63a and 67a tables.

---

## File structure summary

```
10,...                          ← header (1 line, not counted)
69,...                          ← ASD metadata (counted, no PRO_ORDER)
"63a","I",1,...                 ← attributes for match 1  (PRO_ORDER=1, counted)
"67a","I",2,...,"POINT (...)"   ← geometry for match 1   (PRO_ORDER=2, counted)
"63a","I",3,...                 ← attributes for match 2  (PRO_ORDER=3, counted)
"67a","I",4,...,"POLYGON (...)" ← geometry for match 2   (PRO_ORDER=4, counted)
...
99,{N}                          ← N = 1 (type 69) + 2 × (number of matched rows)
```

---

## Output files

A single `usrn-matcher export` run produces four files in `matched_data/`:

| File | Format | Description |
|---|---|---|
| `matched_{name}_ad.csv` | DTF 8.1a | 63a/67a records. Exchange format for NSG-aware tools. |
| `matched_{name}_ad.parquet` | GeoParquet 1.1 | Spatially optimised. One row per feature. DTF column names. Bbox covering + ZSTD + spatial sort. |
| `matched_{name}_ad_flat.csv` | Flat CSV | Same column structure as the parquet, WKT geometry column. Opens in QGIS (Add Delimited Text Layer), Excel, GeoPandas. |
| `matched_{name}_ad.gpkg` | GeoPackage | Same column structure as the parquet, native geometry. Opens in QGIS, ArcGIS, any OGR-compatible tool. |

The parquet, flat CSV and GeoPackage all share the same structure — DTF type 63a field names
(indices 0–11), then the RHS dataset attribute columns, then the geometry. They are all spatially
sorted by the RHS geometry centroid.

---

## CSV deviations from DTF8.1 — summary

For readers comparing the output against the NSG DTF8.1 specification:

| Area | DTF8.1 spec | This file | Reason |
|---|---|---|---|
| `RECORD_IDENTIFIER` for attribution | `63` (integer) | `"63a"` (quoted string) | Signals our extension; avoids confusion with standard type 63 |
| `RECORD_IDENTIFIER` for coordinates | `67` (integer) | `"67a"` (quoted string) | Signals our extension; avoids confusion with standard type 67 |
| Coordinate storage | One type 67 row per vertex (`ASD_X_COORDINATE`, `ASD_Y_COORDINATE`, `COORD_NUMBER`) | One type 67a row per feature (`GEOMETRY_WKT` — full OGC WKT string) | Simpler; one record per feature regardless of vertex count; supports all OGC types |
| `WHOLE_ROAD` | `0` or `1` | Always `0` | Deliberate simplification — all matches treated as part-road; whole-road is not implemented |
| `ASD_COORDINATE` for Points | Spec footnote 52: Points use `ASD_COORDINATE=0`; no type 67 record | Always `1`; Points get a type 67a record | Uniform geometry path for all OGC types |
| Point geometry storage | Inline `SPECIAL_DESIG_START_X/Y` in type 63 (when `ASD_COORDINATE=0`) | WKT `POINT (x y)` in type 67a | Uniform path for all geometry types; consistent with lines and polygons |
| `ASD_GEOMETRY_TYPE` in type 67 | `"L"` or `"P"` only (T1) | `"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"` (T2) | Full OGC simple feature support |
| Coordinate storage | One type 67 row per vertex (`ASD_X_COORDINATE`, `ASD_Y_COORDINATE`, `COORD_NUMBER`) | One type 67a row per feature (`GEOMETRY_WKT` — full OGC WKT string) | Simpler; one record per feature regardless of vertex count; supports all OGC types |
| `SPECIAL_DESIG_START_X/Y` | Present when `ASD_COORDINATE=0` | **Omitted** | We always use `ASD_COORDINATE=1`; inline coords never needed |
| RHS attribute columns | Not in spec (type 63 has fixed fields only) | Appended from index 12 onwards | Carries the matched dataset attributes alongside the USRN record |
| `DTF_VERSION` | `"8.1"` | `"8.1a"` | `"8.1"` prefix is spec-valid; `"a"` suffix signals our extension |
