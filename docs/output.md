# Output

## Standard match (`usrn-matcher match`)

Keeps the **USRN street geometry**. Each row describes a street segment and what was found on or near it.

**Intersect join** — one row per USRN–feature intersection, geometry clipped to the RHS feature (and bbox if supplied):

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | `ST_Intersection` of the USRN and RHS feature — the segment of the street that falls inside the RHS polygon |
| *(RHS columns)* | All selected columns from the RHS dataset |

**Nearest join** — one row per USRN–point pair within `distance_m`, ordered by `usrn, distance_m`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | USRN linestring clipped to the bbox boundary if `--bbox`/`--city` supplied, otherwise full USRN |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the point and the USRN |

---

## DTF export (`usrn-matcher export`)

Keeps the **matched RHS feature geometry**. Each row describes a matched feature from the third-party dataset and which USRN it was matched to.

Four files are written per export run:

| File | Format | Description |
|---|---|---|
| `usrn_{name}_attribution.csv` | DTF 8.1a CSV | Paired type 63a/67a records. Exchange format for NSG-aware tools. |
| `usrn_{name}_attribution.parquet` | GeoParquet 1.1 | Spatially optimised. One row per matched feature. |
| `usrn_{name}_attribution_flat.csv` | Flat CSV | Same columns as parquet, WKT geometry. Opens in QGIS, Excel, GeoPandas. |
| `usrn_{name}_attribution.gpkg` | GeoPackage | Same columns as parquet, native geometry. Opens in QGIS, ArcGIS, OGR tools. |

See [`DTF_MAPPING.md`](DTF_MAPPING.md) for the full DTF8.1 compliance mapping and field layout.

---

## Output cardinality

Both routes run the same spatial join and produce the **same number of rows**. The relationship is many-to-many — a USRN can cross many RHS features, and an RHS feature can touch many USRNs.

| | Intersect (`match`) | Nearest (`match --mode nearest`) | DTF (`export`) |
|---|---|---|---|
| Geometry kept | `ST_Intersection(usrn, rhs)` — segment of the USRN inside the RHS polygon | USRN clipped to bbox if supplied, otherwise full USRN | Full unclipped RHS feature geometry |
| Repeated entity | USRNs — same USRN appears once per RHS feature it crosses | RHS features — same feature appears once per USRN it touches | RHS features |
| To get unique streets | `GROUP BY usrn` | `GROUP BY usrn` | `GROUP BY usrn` |
| To get unique RHS features | Deduplicate on RHS attribute columns | `GROUP BY` RHS attribute columns | `GROUP BY` RHS attribute columns |
| Question answered | What portion of this street falls within each RHS feature? | Which streets are near this feature? | — |

**Example — soil data for Leeds (39,582 rows):**

- A soil polygon covering a large area may touch 50+ USRNs → appears 50+ times in the output
- A long A-road crossing 10 soil polygons → appears 10 times in both outputs, each with a different soil type
- To count how many soil types each USRN crosses: `GROUP BY usrn, COUNT(DISTINCT MAP_SYMBOL)`
- To count how many USRNs each soil polygon touches: `GROUP BY MAP_SYMBOL, COUNT(DISTINCT usrn)`
