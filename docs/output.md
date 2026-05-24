# Output

## Standard match (`usrn-matcher match`)

**Intersect join** — one row per USRN–feature intersection:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | Present only when `--geometry` is not `none` |
| *(RHS columns)* | All selected columns from the RHS dataset |

**Nearest join** — one row per USRN–point pair within `--distance`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | Present only when `--geometry` is not `none` |
| *(RHS columns)* | All selected columns from the RHS dataset |
| `distance_m` | Distance in metres between the point and the USRN |

**Geometry modes (`--geometry`):**

| Mode | Default | Description |
|---|---|---|
| `none` | ✓ | No geometry — fastest, attribute-only |
| `usrn` | | Full USRN linestring (clipped to bbox if supplied) |
| `clip` | | `ST_Intersection(usrn, rhs_polygon)` — intersect only |
| `rhs` | | Full unclipped RHS feature geometry — required for DTF export |

---

## DTF export (`usrn-matcher dtf-export`)

Four files written per run (stem = `matched_{name}_ad`):

| File | Format | Description |
|---|---|---|
| `matched_{name}_ad.csv` | DTF 8.1a CSV | Type 70 records. Exchange format for NSG-aware tools. |
| `matched_{name}_ad.parquet` | GeoParquet 1.1 | Hilbert-sorted, ZSTD-compressed. |
| `matched_{name}_ad_flat.csv` | Flat CSV | WKT geometry column. Opens in QGIS, Excel, GeoPandas. |
| `matched_{name}_ad.gpkg` | GeoPackage | Native geometry. Opens in QGIS, ArcGIS, any OGR tool. |

See [dtf-mapping.md](dtf-mapping.md) for the full DTF8.1 field layout.

---

## Cardinality

The join is many-to-many. A USRN can cross many RHS features; an RHS feature can touch many USRNs.

**Example — soil data for Leeds (39,582 rows):**
- A soil polygon covering a large area may touch 50+ USRNs → appears 50+ times
- A long A-road crossing 10 soil types → appears 10 times, each with a different soil attribute
- Unique streets: `GROUP BY usrn`
- Unique RHS features: `GROUP BY` RHS attribute columns
