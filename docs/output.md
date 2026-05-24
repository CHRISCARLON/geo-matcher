# Output

All match results are attribute-only (no geometry column). Output is a tabular join of USRNs to RHS dataset attributes.

---

## Polygon join

One row per USRN–feature intersection:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| *(RHS columns)* | All selected columns from the RHS dataset |

---

## Point join

One row per USRN–point pair within `--distance`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the point and the USRN |

---

## Line join

One row per USRN–line pair:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the USRN and the RHS line |
| `is_intersection` | `true` if the pair came from the Phase 1 intersect pass |
| `overlap_length_pct` | Corridor overlap score: intersection length with the USRN buffer ÷ max(line length, 2 × distance_m). Higher = better alignment. `0.0` for Phase 3 rows. |
| `match_phase` | `1` = Phase 1 intersect, `2` = Phase 2 corridor, `3` = Phase 3 nearest fallback |

---

## Cardinality

The join is many-to-many. A USRN can cross many RHS features; an RHS feature can touch many USRNs.

- Unique streets: `GROUP BY usrn`
- Unique RHS features: `GROUP BY` RHS attribute columns
