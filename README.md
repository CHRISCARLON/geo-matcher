# USRN Soil Matcher

Spatially joins Unique Street Reference Numbers (USRNs) to National Soil polygons.

Built on SedonaDB (Rust-based spatial query engine).

Just focuses on the National Soil polygons for now but will become more generic.

## Installation

Will put on PyPi soon.

```bash
git clone <repo>
cd usrn-matcher
uv sync
```

Place your input files in `input_data/` before running:

```
input_data/
  osopenusrn.gpkg
  NationalSoilMap.gpkg
```

## Usage

```bash
usrn-matcher --bbox XMIN YMIN XMAX YMAX [--output csv|parquet|sample] [--explain]

or 

usrn-matcher --city LEEDS [--output csv|parquet|sample] [--explain]
```

A bounding box is required — full-dataset joins are not permitted.

**Examples**
```bash
# London
usrn-matcher --bbox 503000 155000 562000 200000

# Leeds with query plan
usrn-matcher --city LEEDS --explain
```

**As a library**
```python
from usrn_soil_matcher import UsrnSoilMatcher

matcher = UsrnSoilMatcher.from_gpkgs(
    usrn_gpkg="input_data/osopenusrn.gpkg",
    soil_gpkg="input_data/NationalSoilMap.gpkg",
)
gdf = matcher.match(bbox=[412000, 426000, 444000, 445000])
matcher.to_csv(gdf, "output.csv")
```

Coordinates are in EPSG:27700 (British National Grid).

---

## Things I've learnt so far

These are just some notes to record how best to structure geoparquet files to make use of how SedonaDB works internally.

Might still be some errors in my understanding!

### 1. GeoParquet 1.1 with bbox covering columns

Each parquet file contains a `bbox` struct column (`xmin`, `ymin`, `xmax`, `ymax`) with one row per geometry, computed from `gdf.geometry.bounds`. 

Parquet files automatically write min/max statistics on these float columns into the file footer at the row group level.

The `geo` metadata is patched to GeoParquet 1.1 with a `covering` key:

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

This tells SedonaDB which columns contain per-row bbox data. 

Without this key, Sedona ignores the bbox columns entirely. 

The implementation is in `sedona-geoparquet/src/file_opener.rs` — `parse_column_coverings()` maps these paths to parquet column indices, `row_group_covering_geo_stats()` reads the min/max stats, and `filter_access_plan_using_geoparquet_covering()` calls `access_plan.skip(i)` for row groups outside the query bbox.

**Result:** For a Leeds bbox query, 858/979 USRN row groups are skipped before any geometry bytes are read (88% pruning). Bytes scanned drops from 162 MB to around 20 MB.

### 2. Fine-grained row groups

USRNs are written with `row_group_size=20,000` (89 row groups across 1.76M rows) and soil with `row_group_size=10,000` (5 row groups across 42K rows). 

More row groups = more opportunities to prune. `pq.write_table(..., row_group_size=N)` is used directly rather than `geoarrow.rust.io.GeoParquetWriter`, which has no `row_group_size` parameter and collapses data to 1–2 row groups via internal byte buffering.

TODO: Explore how I could use the rust arrow io crate and add row group sizes - speak to someone about this/raise PR?

### 3. Spatial sort

Before writing, geometries are sorted by `gdf.sort_values("geometry")`. 

GeoPandas sorts by the geometry's WKB representation which approximates a spatial ordering — geographically nearby features end up in the same row groups. 

This maximises bbox pruning effectiveness: a Leeds query skips row groups containing only southern England roads without inspecting a single geometry.

### 4. ZSTD compression

All columns are written with `compression="zstd"`. String columns (soil type, drainage class etc.) use `RLE_DICTIONARY` encoding automatically. Both reduce bytes read from disk during scans.

### 5. Prepared build-side geometries (R-tree + prepared geometry index)

```python
sd.sql("SET sedona.spatial_join.execution_mode TO 'prepare_build'").execute()
```

SedonaDB builds an **R-tree with Hilbert curve sorting** on the soil side (the build side — smaller table, 42K polygons) before the join starts. 

Each soil polygon is also parsed into a prepared geometry (GEOS `PreparedGeometry` or TG internal index) which pre-computes internal spatial indices.

The join then has two phases for each USRN:

1. **Index phase** — for every USRN, Sedona computes its bounding rectangle and fires a single R-tree search (`default_spatial_index.rs:316`):
   ```rust
   let mut candidates = self.inner.rtree.search(min.x, min.y, max.x, max.y);
   ```
   This returns integer IDs of every soil polygon whose bounding box overlaps the USRN's bbox. For example, a USRN in Leeds at `[413000, 432000, 413200, 432050]` might get back `[8, 47, 203]` — three soil polygons whose bboxes touch that area. Everything else in the R-tree is discarded without touching any WKB bytes.

2. **Refinement phase** — `ST_Intersects` is evaluated only on those 3 candidates using the pre-built prepared geometries. The candidate IDs are looked up via `data_id_to_batch_pos` → `(batch_idx, row_idx)` to retrieve the WKB, then the exact predicate is evaluated. Without `prepare_build`, the WKB would be re-parsed from scratch on every evaluation.

`execution_mode=1` in the EXPLAIN output confirms `PrepareBuild` is active. Source: `sedona-common/src/option.rs:279`.

### 6. Geometry clipping (ST_Intersection)

The join uses `ST_Intersection(u.geometry, s.geometry)` rather than returning the full USRN geometry. 

A USRN crossing three soil polygons produces three rows, each carrying only the segment that actually falls within that soil type. 

When a bbox is supplied, the result is also clipped to the bbox boundary so long USRNs don't bleed outside the area of interest:

```sql
ST_Intersection(ST_Intersection(u.geometry, s.geometry), bbox_polygon)
```

The clipping happens in `ProjectionExec` (post-join) on the 25K matched rows — not pre-join on 1.76M rows!!

---

## Output

Each row in the output represents a **segment** of a USRN within a single soil polygon, with the clipped geometry and all soil attributes (drainage class, soilscape, fertility, geology etc.).

| Column | Description |
|---|---|
| `usrn` | Unique Street Reference Number |
| `street_type` | Road classification |
| `geometry` | Clipped linestring (WKT) |
| `DRAINAGE` | Soil drainage class |
| `SOILSCAPE` | Soilscape description |
| `GEOLOGY` | Underlying geology |
| `FERTILITY` | Agricultural fertility |
| … | Other soil attributes |
