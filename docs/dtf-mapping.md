# DTF8.1 Field Mapping

`usrn_matcher.dtf` produces a **community extension to DTF8.1** for third-party spatially matched datasets. The fixed fields (record identifiers, PRO_ORDER, USRN, dates) are DTF-compliant; the extension adds matched dataset attributes and inline WKT geometry as trailing fields so existing NSG-aware tooling can parse the fixed portion without modification.

`DTF_VERSION` is written as `"8.1a"` to signal the extension to downstream consumers.

---

## Type 10 — Header

| Field | Value |
|---|---|
| `RECORD_IDENTIFIER` | `10` |
| `SWA_ORG_NAME_TEXT` | `--dtf-org-name` |
| `SWA_ORG_REF` | `--dtf-org-ref` |
| `PROCESS_DATE` / `ENTRY_DATE` | today |
| `DTF_VERSION` | `"8.1a"` |
| `FILE_TYPE` | `"F"` (Full Supply) |

---

## Type 69 — ASD Metadata

The 24 MD_* coverage percentage fields (indices 14–37) are all `0` — this file carries a third-party matched dataset, not standard special designations, so coverage percentages are not meaningful.

| Field | Value |
|---|---|
| `RECORD_IDENTIFIER` | `69` |
| `LINKED_DATA` | RHS dataset name (e.g. `"stops"`) |
| `CUSTODIAN_NAME` | `--dtf-org-name` |
| `CO_ORD_SYSTEM` | `"British National Grid"` |
| MD_* fields | `0` |

---

## Type 70 — USRN Attribution Record

`RECORD_IDENTIFIER = "70"` is not a standard DTF8.1 type. It combines the type 63 (Special Designation) fixed fields with inline WKT geometry — one record per matched row, no separate coordinate record.

**Deliberate simplifications:**
- `WHOLE_ROAD = 0` always (all matches treated as part-road)
- `ASD_COORDINATE` / `ASD_COORDINATE_COUNT` omitted (type 70 carries geometry inline)

### Field layout

| Index | Field | Value |
|---|---|---|
| 0 | `RECORD_IDENTIFIER` | `"70"` |
| 1 | `CHANGE_TYPE` | `"I"` |
| 2 | `PRO_ORDER` | global sequential counter |
| 3 | `USRN` | street reference number |
| 4 | `ATTRIBUTION_SEQ_NUM` | sequential per USRN (1, 2, 3 …) |
| 5 | `ATTRIBUTION_SOURCE_NAME` | RHS dataset name |
| 6 | `WHOLE_ROAD` | `0` |
| 7 | `RECORD_START_DATE` | today |
| 8 | `LAST_UPDATE_DATE` | today |
| 9 | `GEOMETRY_TYPE` | `"PT"`, `"L"`, `"ML"`, `"P"`, `"MP"` |
| 10+ | *RHS attribute columns* | auto-encoded from matched dataset |
| N (last) | `GEOMETRY_WKT` | full OGC WKT geometry string |

### Example

Three bus stops matched to the same USRN:

```
"70","I",1,5300050,1,"stops",0,2026-04-25,2026-04-25,"PT","490001234A","Acacia Avenue","POINT (413064 429030)"
"70","I",2,5300050,2,"stops",0,2026-04-25,2026-04-25,"PT","490001235A","Acacia Avenue","POINT (413080 429045)"
"70","I",3,5300050,3,"stops",0,2026-04-25,2026-04-25,"PT","490001236A","Acacia Avenue","POINT (413100 429060)"
```

---

## Type 99 — Trailer

| Field | Value |
|---|---|
| `RECORD_IDENTIFIER` | `99` |
| `RECORD_COUNT` | count of body records (type 69 + all type 70) |

Type 10 and type 99 are excluded from the count.

---

## PRO_ORDER

Global sequential integer assigned to every body record. Starts at `1` for the first type 70; type 69 has no PRO_ORDER but is included in the trailer count.

```
69,...                   ← no PRO_ORDER, counted in trailer
"70","I",1,...           ← PRO_ORDER = 1
"70","I",2,...           ← PRO_ORDER = 2
99,{N}                   ← N = 1 (type 69) + number of type 70 rows
```
