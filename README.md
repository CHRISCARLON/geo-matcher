# usrn-matcher

Spatially join Unique Street Reference Numbers (USRNs) to any polygon or point dataset using SedonaDB.

Built on [Apache Sedona](https://sedona.apache.org/) (Rust-based spatial query engine) with optimised [GeoParquet 1.1](https://geoparquet.org/) files (he hopes).

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

**Intersect join** — one row per USRN–polygon intersection, geometry clipped to the polygon (and bbox if supplied):

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | Clipped USRN linestring (WKT in CSV, WKB in GeoParquet) |
| *(RHS columns)* | All selected columns from the right-hand side dataset |

**Nearest join** — one row per USRN–point pair within `distance_m`, ordered by `usrn, distance_m`:

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | USRN linestring (WKT in CSV, WKB in GeoParquet) |
| *(RHS columns)* | All selected columns from the right-hand side dataset |
| `distance_m` | Distance in metres between the point and the USRN |
