# usrn-matcher

Spatially join Unique Street Reference Numbers (USRNs) to any geospatial dataset using SedonaDB.

Built on [Apache Sedona](https://sedona.apache.org/) (Rust-based spatial query engine) with optimised [GeoParquet 1.1](https://geoparquet.org/) files.

## What it does

`usrn-matcher` answers the question: *which USRN is this feature on or near?*

Given a third-party spatial dataset (naptan data, national soil data — anything with geometry), it finds the USRN or USRNs that intersect or are nearest to each feature and produces a joined output carrying both the USRN and the original dataset's attributes.

There are two output routes, each keeping a different geometry:

| Route | Command | Geometry kept | Best for |
|---|---|---|---|
| **Standard** | `usrn-matcher match` | USRN street geometry | Street-centric analysis — each row describes a street segment |
| **DTF export** | `usrn-matcher export` | Matched RHS feature geometry | Dataset-centric exchange — each row describes a matched feature from the third-party dataset, in a format close to DTF8.1 |

The DTF export is a community extension to the NSG DTF8.1 format for third-party spatially matched datasets — see [`DTF_MAPPING.md`](DTF_MAPPING.md) for the full compliance mapping.

## Installation (will add it to pypi soon)

```bash
git clone <repo>
cd usrn-matcher
uv sync
```

## Quick start

### 1. Initialise project directories

```bash
usrn-matcher init
```

Creates `input_data/`, `output_data/`, and `matched_data/` if they don't exist:

```
input_data/     ← place your source GeoPackages here
output_data/    ← prepared GeoParquet files are stored here
matched_data/   ← join results are written here
```

### 2. Pre-spatial phase — prepare GeoParquet files

Convert your source files into optimised GeoParquet 1.1 with bbox covering columns and spatial sorting. 

This only needs to be done once per dataset.

```bash
usrn-matcher prepare \
  --usrn-gpkg input_data/osopenusrn.gpkg \
  --rhs-gpkg input_data/NationalSoilMap.gpkg \
  --rhs-name soil \
  --rhs-geometry-col SHAPE
```

Key options:

| Option | Default | Description |
|---|---|---|
| `--usrn-gpkg` | `input_data/osopenusrn.gpkg` | OS Open USRN GeoPackage |
| `--rhs-gpkg` | required | Right-hand side source file |
| `--rhs-name` | required | Short identifier (valid SQL identifier, e.g. `soil`, `flood_risk`) |
| `--rhs-geometry-col` | `geometry` | Geometry column name in source file |
| `--rhs-row-group-size` | `10000` | Row group size for RHS GeoParquet |
| `--usrn-row-group-size` | `20000` | Row group size for USRN GeoParquet |
| `--cache-dir` | `output_data` | Directory for cached GeoParquet files |
| `--force` | off | Re-prepare even if GeoParquet already exists |

### 3. Spatial phase — run the join

```bash
# Full national intersect join (polygon/line datasets)
usrn-matcher match --rhs-name soil

# Nearest-USRN join (point datasets) — assigns each point to its closest USRN within 25m
usrn-matcher match --rhs-name stops --mode nearest --distance 25 --city LEEDS

# Restricted to a bounding box (EPSG:27700)
usrn-matcher match --rhs-name soil --bbox 412000 426000 444000 445000

# Named city
usrn-matcher match --rhs-name soil --city LEEDS

# Select specific columns from the RHS
usrn-matcher match --rhs-name soil --city LEEDS --rhs-columns MUSID MAP_SYMBOL DRAINAGE

# Output as GeoParquet instead of CSV
usrn-matcher match --rhs-name soil --city LEEDS --output parquet

# Sample the first 10,000 rows
usrn-matcher match --rhs-name soil --output sample --sample-rows 10000

# Inspect the query plan
usrn-matcher match --rhs-name stops --mode nearest --city LEEDS --explain
```

Key options:

| Option | Default | Description |
|---|---|---|
| `--rhs-name` | required | Name of the prepared RHS dataset |
| `--rhs-columns` | all | Columns to select (auto-discovers from schema if omitted) |
| `--mode` | `intersect` | `intersect` for polygon/line datasets; `nearest` for point datasets |
| `--distance` | `50` | Search radius in metres for `--mode nearest` |
| `--bbox XMIN YMIN XMAX YMAX` | full join | Bounding box in EPSG:27700 |
| `--city` | full join | Named city preset (LEEDS, LONDON, MANCHESTER, …) |
| `--output` | `csv` | `csv`, `parquet`, or `sample` |
| `--sample-rows` | `100000` | Row limit for `--output sample` |
| `--explain` | off | Run EXPLAIN ANALYZE before the join and log the query plan |
| `--cache-dir` | `output_data` | Directory containing prepared GeoParquet files |
| `--matched-dir` | `matched_data` | Directory for output files |

Output files are named `usrn_{rhs-name}_attribution.{ext}`.

### 4. DTF export — matched feature geometry in DTF8.1-inspired format

Runs the spatial join and writes four output files, each carrying the **matched RHS feature geometry** (not the USRN geometry):

```bash
# Intersect join (polygon/line datasets)
usrn-matcher export \
  --rhs-name soil \
  --city LEEDS \
  --dtf-org-name "My Council" \
  --dtf-org-ref 1234

# Nearest join (point datasets)
usrn-matcher export \
  --rhs-name stops \
  --mode nearest \
  --distance 25 \
  --city LEEDS \
  --dtf-org-name "My Council" \
  --dtf-org-ref 1234
```

Key options:

| Option | Default | Description |
|---|---|---|
| `--rhs-name` | required | Name of the prepared RHS dataset |
| `--mode` | `intersect` | `intersect` or `nearest` |
| `--distance` | `50` | Search radius in metres for `--mode nearest` |
| `--bbox XMIN YMIN XMAX YMAX` | full join | Bounding box in EPSG:27700 |
| `--city` | full join | Named city preset |
| `--dtf-org-name` | `usrn-matcher` | Organisation name in the DTF type 10 header |
| `--dtf-org-ref` | `0` | SWA organisation reference code |
| `--cache-dir` | `output_data` | Prepared GeoParquet directory |
| `--matched-dir` | `matched_data` | Output directory |

Output files written to `matched_data/`:

| File | Format | Description |
|---|---|---|
| `matched_{name}_ad.csv` | DTF 8.1a CSV | Interleaved type 63a/67a records. Exchange format for NSG-aware tools. |
| `matched_{name}_ad.parquet` | GeoParquet 1.1 | Spatially optimised. One row per matched feature. |
| `matched_{name}_ad_flat.csv` | Flat CSV | Same columns as parquet, WKT geometry. Opens in QGIS, Excel, GeoPandas. |
| `matched_{name}_ad.gpkg` | GeoPackage | Same columns as parquet, native geometry. Opens in QGIS, ArcGIS, OGR tools. |

See [`DTF_MAPPING.md`](DTF_MAPPING.md) for the full DTF8.1 compliance mapping and field layout.

---

## Programmatic usage

```python
from usrn_matcher import UsrnMatcher, DatasetConfig
from usrn_matcher.prepare import prepare_usrns, prepare_dataset

# Describe the right-hand side dataset
cfg = DatasetConfig(
    name="soil",
    source_path="input_data/NationalSoilMap.gpkg",
    geometry_column="SHAPE",               # rename non-standard geometry column
    columns=["MAP_SYMBOL", "DRAINAGE"],    # [] = auto-select all columns
    row_group_size=10_000,
)

# Pre-spatial phase (skipped if GeoParquet already exists)
matcher = UsrnMatcher.from_sources(
    usrn_gpkg="input_data/osopenusrn.gpkg",
    rhs_config=cfg,
    cache_dir="output_data",
)

# Spatial phase
table = matcher.match_intersect(bbox=[412000, 426000, 444000, 445000])
matcher.to_csv(table, "matched_data/usrn_soil_attribution.csv")

# Or skip preparation if GeoParquet files are already prepared
matcher = UsrnMatcher(
    usrn_parquet="output_data/usrns_27700.parquet",
    rhs_config=cfg,
)
table = matcher.match_intersect()  # full national join
```

### DTF export

```python
from usrn_matcher import UsrnMatcher, DatasetConfig, DTFConfig
from usrn_matcher.dtf import to_dtf_csv, to_dtf_geoparquet, to_dtf_flat_csv, to_dtf_gpkg
import pathlib

cfg = DatasetConfig(
    name="stops",
    source_path="input_data/naptan_stops.gpkg",
    columns=["ATCOCode", "CommonName", "StopType"],
    row_group_size=10_000,
)

dtf_cfg = DTFConfig(
    swa_org_name="My Council",
    swa_org_ref=1234,
    rhs_name="stops",
)

matcher = UsrnMatcher(
    usrn_parquet="output_data/usrns_27700.parquet",
    rhs_config=cfg,
)

# Run nearest join — must pass include_rhs_geometry=True for DTF export
table = matcher.match_nearest(
    bbox=[412000, 426000, 444000, 445000],
    distance_m=25,
    include_rhs_geometry=True,
)

out = pathlib.Path("matched_data")
stem = "matched_stops_ad"

to_dtf_csv(table, dtf_cfg, out / f"{stem}.csv")            # DTF 8.1a CSV
to_dtf_geoparquet(table, dtf_cfg, out / f"{stem}.parquet") # GeoParquet 1.1
to_dtf_flat_csv(table, dtf_cfg, out / f"{stem}_flat.csv")  # flat CSV (QGIS-ready)
to_dtf_gpkg(table, dtf_cfg, out / f"{stem}.gpkg")          # GeoPackage
```

---

## How it works

### Pre-spatial phase

Each source file is converted to an optimised GeoParquet 1.1 file in `output_data/`. 

Run it once, then query as many times as you like.

**Spatial sort** — geometries are sorted by WKB representation before writing. This approximates a spatial ordering so geographically nearby features land in the same row groups.

**Fine-grained row groups** — USRNs use `row_group_size=20,000` (89 row groups across 1.76M rows); polygon datasets default to `10,000`. More row groups means more opportunities for SedonaDB to skip irrelevant data.

**GeoParquet 1.1 bbox covering columns** — a `bbox` struct column (`xmin`, `ymin`, `xmax`, `ymax`) is added to every row. Parquet writes min/max statistics on these floats into the file footer at the row group level. The geo metadata is patched with a `covering` key:

```json
"covering": {
  "bbox": {
    "xmin": ["bbox", "xmin"],
    "ymin": ["bbox", "ymin"],
    "xmax": ["bbox", "xmax"],
    "ymax": ["bbox", "ymax"]
  }
}
```

SedonaDB reads this and calls `access_plan.skip(i)` for any row group whose bbox doesn't overlap the query region — before reading a single geometry byte. For a Leeds query, 858/979 USRN row groups are skipped (88% pruning, ~162 MB → 20 MB scanned).

Two parquet optimisations are in play here:

**Predicate pushdown** — the bbox covering columns enable row group skipping. SedonaDB checks the `xmin/ymin/xmax/ymax` min/max statistics in the file footer for each row group and calls `access_plan.skip(i)` for any group whose bbox doesn't overlap the query. No WKB bytes are read for skipped row groups — this is the 88% pruning (858/979 row groups) measured for a Leeds query. See the [Polars predicate pushdown post](https://pola.rs/posts/predicate-pushdown-query-optimizer/) for a good general breakdown of the technique.

**Projection pushdown** — because parquet is columnar, selecting only `usrn`, `street_type`, `geometry` and the chosen RHS columns means the reader fetches only those column chunks from disk. Every column we don't select is never touched. This is free — it follows directly from the columnar layout.

The [Apache Arrow blog post on querying parquet with millisecond latency](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/) is a good deep-dive into how parquet enables both of these at the file-format level.

It's like a poor man's spatial index essentially.

**ZSTD compression** — all columns compressed with ZSTD; low-cardinality string columns use `RLE_DICTIONARY` encoding automatically.

### Spatial phase

**Two-phase spatial join (R-tree + refinement)**

SedonaDB executes each join in two phases:

1. **Index phase** — an R-tree built with Hilbert curve ordering finds candidate geometry pairs from their bounding rectangles, without touching WKB bytes.
2. **Refinement phase** — the exact spatial predicate (`ST_Intersects` or `ST_DWithin`) is evaluated only on candidates.

**Build/probe side assignment**

Sedona automatically assigns the smaller table to the build side (R-tree index) and the larger to the probe side, based on cardinality estimates (`should_swap_join_order` in `physical_planner.rs`). For stops (434K) vs USRNs (1.76M): stops = build, USRNs = probe.

**Speculative execution mode**

Sedona's default `execution_mode` is `Speculative(N)`. It samples the first N probe-side geometries at runtime and picks the best refinement strategy:

- `prepare_build` — lazily creates GEOS `PreparedGeometry` objects for build-side geometries on first use, caching them for reuse across all probe comparisons. Worth it for complex polygons.
- `prepare_probe` — prepares probe-side geometries instead. Better when the probe side has complex geometry.
- `prepare_none` — no prepared geometries. Optimal for simple geometry types like points.

In practice, Speculative chooses `prepare_none` (`execution_mode=0`) for point datasets (e.g. stops) since point geometries are trivial to evaluate directly. We do not override this setting.

**Geometry clipping (intersect join)**

`ST_Intersection(u.geometry, s.geometry)` is used rather than returning full USRN geometries. A USRN crossing three polygons produces three rows, each with only the segment inside that polygon. When a bbox is supplied the result is also clipped to its boundary.

**ST_AsWKB wrapping (nearest join)**

The nearest join wraps the USRN geometry as `ST_AsWKB(u.geometry)` rather than selecting it as a raw column. Selecting a raw `WkbView` geometry column causes a segfault in Sedona's `to_arrow_table()` — the computed expression forces a safe buffer allocation. See `sedona-spatial-join/src/refine/geos.rs` for the underlying materialisation path.

---

## Output

### Standard match output (`usrn-matcher match`)

Keeps the **USRN street geometry**. Each row describes a street segment and what was found on or near it.

**Intersect join** — one row per USRN–feature intersection, geometry clipped to the RHS feature (and bbox if supplied):

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | `ST_Intersection` of the USRN and RHS feature — the segment of the street that falls inside the RHS polygon (WKT in CSV, WKB in GeoParquet) |
| *(RHS columns)* | All selected columns from the right-hand side dataset |

**Nearest join** — one row per USRN–point pair within `distance_m`, ordered by `usrn, distance_m`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | USRN linestring clipped to the bbox boundary if `--bbox`/`--city` supplied, otherwise full USRN (no RHS clipping — points have no area to intersect against) |
| *(RHS columns)* | All selected columns from the right-hand side dataset |
| `distance_m` | Distance in metres between the point and the USRN |

### DTF export output (`usrn-matcher export`)

Keeps the **matched RHS feature geometry**. Each row describes a matched feature from the third-party dataset and which USRN it was matched to. Four files are written per export run — see the export section above for the full file list.

---

### Output cardinality

Both routes run the same spatial join and produce the **same number of rows**. The relationship is many-to-many — a USRN can cross many RHS features, and an RHS feature can touch many USRNs — so the output will always have more rows than either source dataset alone.

The difference is which entity is repeated across rows:

| | Normal intersect (`match`) | Normal nearest (`match --mode nearest`) | DTF (`export`) |
|---|---|---|---|
| Geometry kept | `ST_Intersection(usrn, rhs)` — segment of the USRN inside the RHS polygon, also clipped to bbox if supplied | USRN clipped to bbox if supplied, otherwise full USRN — no RHS clipping (points have no area) | Full unclipped RHS feature geometry |
| Repeated entity | USRNs — same USRN appears once per RHS feature it crosses | RHS features — same feature appears once per USRN it touches |
| To get unique streets | `GROUP BY usrn` | `GROUP BY usrn` |
| To get unique RHS features | Deduplicate on RHS attribute columns | `GROUP BY` RHS attribute columns |
| Question answered | What portion of this street falls within each RHS feature? | Which streets does this feature touch? |

**Example — soil data for Leeds (39,582 rows):**
- A soil polygon covering a large area may touch 50+ USRNs → appears 50+ times in the DTF output
- A long A-road crossing 10 soil polygons → appears 10 times in both outputs, each with a different soil type
- To count how many soil types each USRN crosses: `GROUP BY usrn, COUNT(DISTINCT MAP_SYMBOL)`
- To count how many USRNs each soil polygon touches: `GROUP BY MAP_SYMBOL, COUNT(DISTINCT usrn)`
