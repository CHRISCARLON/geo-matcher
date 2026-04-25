# Output

## Standard match (`usrn-matcher match`)

Produces attribute-only output by default (`--geometry none`). Add `--geometry <mode>` to include geometry.

**Intersect join** — one row per USRN–feature intersection:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | Only present when `--geometry` is not `none` — see geometry modes below |
| *(RHS columns)* | All selected columns from the RHS dataset |

**Nearest join** — one row per USRN–point pair within `distance_m`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | Only present when `--geometry` is not `none` — see geometry modes below |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the point and the USRN |

**Geometry modes (`--geometry`):**

| Mode | Default | Geometry returned |
|---|---|---|
| `none` | ✓ | No geometry column — fastest, attribute-only output |
| `usrn` | | Full USRN linestring (clipped to bbox if `--bbox`/`--city` supplied) |
| `clip` | | `ST_Intersection(usrn, rhs)` — segment of the USRN inside the RHS polygon (intersect only) |
| `rhs` | | Full unclipped RHS feature geometry — required for DTF export |

---

## DTF export (`usrn-matcher dtf-export`)

Keeps the **matched RHS feature geometry**. Each row describes a matched feature from the third-party dataset and which USRN it was matched to. Always runs with `geometry="rhs"` internally.

Four files are written per export run (stem = `matched_{name}_ad`):

| File | Format | Description |
|---|---|---|
| `matched_{name}_ad.csv` | DTF 8.1a CSV | Type 70 records (one per matched feature). Exchange format for NSG-aware tools. |
| `matched_{name}_ad.parquet` | GeoParquet 1.1 | Spatially optimised. One row per matched feature. Hilbert-sorted, ZSTD-compressed. |
| `matched_{name}_ad_flat.csv` | Flat CSV | Same columns as parquet, WKT geometry. Opens in QGIS, Excel, GeoPandas. |
| `matched_{name}_ad.gpkg` | GeoPackage | Same columns as parquet, native geometry. Opens in QGIS, ArcGIS, OGR tools. |

See [`dtf-mapping.md`](dtf-mapping.md) for the full DTF8.1 compliance mapping and field layout.

---

## Output cardinality

Both routes run the same spatial join and produce the **same number of rows**. The relationship is many-to-many — a USRN can cross many RHS features, and an RHS feature can touch many USRNs.

| | Intersect (`match`) | Nearest (`match --mode nearest`) | DTF (`dtf-export`) |
|---|---|---|---|
| Geometry kept | Depends on `--geometry` (default: none) | Depends on `--geometry` (default: none) | Full unclipped RHS feature geometry |
| Repeated entity | USRNs — same USRN appears once per RHS feature it crosses | RHS features — same feature appears once per USRN it touches | RHS features |
| To get unique streets | `GROUP BY usrn` | `GROUP BY usrn` | `GROUP BY usrn` |
| To get unique RHS features | Deduplicate on RHS attribute columns | `GROUP BY` RHS attribute columns | `GROUP BY` RHS attribute columns |
| Question answered | What portion of this street falls within each RHS feature? | Which streets are near this feature? | — |

**Example — soil data for Leeds (39,582 rows):**

- A soil polygon covering a large area may touch 50+ USRNs → appears 50+ times in the output
- A long A-road crossing 10 soil polygons → appears 10 times in both outputs, each with a different soil type
- To count how many soil types each USRN crosses: `GROUP BY usrn, COUNT(DISTINCT MAP_SYMBOL)`
- To count how many USRNs each soil polygon touches: `GROUP BY MAP_SYMBOL, COUNT(DISTINCT usrn)`
