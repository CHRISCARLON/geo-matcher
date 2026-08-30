# Output

All match results are attribute-only (no geometry column). 

Output is a tabular join of USRNs (or UPRNs, via `--lhs-name uprn`) to RHS dataset attributes.

---

## Polygon join (`--lhs-name usrn`, default)

One row per USRN–feature intersection:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| *(RHS columns)* | All selected columns from the RHS dataset |

---

## UPRN polygon join (`--lhs-name uprn`)

One row per UPRN that falls inside an RHS polygon — a plain point-in-polygon
join, no `street_type`/`distance_m`/phase columns (those are USRN-specific
concepts with no UPRN equivalent):

| Column | Description |
|---|---|
| `uprn` | Unique Property Reference Number |
| *(RHS columns)* | All selected columns from the RHS dataset |

Cardinality differs from the USRN polygon join too: each UPRN is a single
point, so it appears at most once per RHS polygon it falls inside (zero rows
at all if it falls inside none — no explicit "unmatched" row is emitted,
unlike USRN's phased line join). If the RHS polygons overlap, a UPRN inside
the overlap gets one row per overlapping polygon.

---

## Point join (`--lhs-name usrn`, default)

One row per USRN–point pair within `--distance`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the point and the USRN |

---

## Line join (`--lhs-name usrn`, default)

One row per USRN–line pair:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the USRN and the RHS line |
| `is_intersection` | `true` if the pair came from the Phase 1 intersect pass |
| `overlap_length_pct` | Corridor overlap score: intersection length with the USRN buffer ÷ max(line length, 2 × distance_m). Higher = better alignment. `0.0` for Phase 3 and Phase 4 rows. |
| `match_phase` | `1` = Phase 1 intersect, `2` = Phase 2 corridor, `3` = Phase 3 nearest fallback, `4` = Phase 4 connectivity inheritance |

---

## Cardinality (`--lhs-name usrn`, default)

The join is many-to-many. A USRN can cross many RHS features; an RHS feature can touch many USRNs.

- Unique streets: `GROUP BY usrn`
- Unique RHS features: `GROUP BY` RHS attribute columns

`match_phase` describes the **pair**, not the feature. Phases 1 and 2 both run over every
line, so a single feature can appear under `match_phase = 1` for the street it crosses and
`match_phase = 2` for a street it runs alongside. Phases 3 and 4 are fallbacks and only
ever apply to features that matched in neither of the first two, so:

- Each `(feature, usrn)` pair appears exactly once.
- Counting matched features means `COUNT(DISTINCT <rhs id>)`, not summing per-phase counts.

`--lhs-name uprn`'s cardinality is simpler and covered inline in the UPRN
polygon join section above, not here — no phases, no many-to-many street
crossings, just one point per matching polygon.
