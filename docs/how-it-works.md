# How it works

## Pre-spatial phase (prepare)

Each source file is converted to an optimised GeoParquet 1.1 file in `output_data/`. Run once, then query as many times as you like.

**DuckDB pipeline** — For GeoPackage/Shapefile sources, `ST_Read()` reads the file natively. For CSV sources, `read_csv()` ingests the file and `ST_Point()` builds point geometries from the X/Y columns. DuckDB then sorts, computes the bbox struct, and writes the Parquet file in a single `COPY ... TO ... (FORMAT PARQUET)` statement. A lightweight PyArrow post-processing step patches the GeoParquet 1.1 metadata (covering key, CRS PROJJSON) into the file footer.

**Spatial sort** — Geometries are sorted by a Hilbert curve key computed from each feature's centroid within the British National Grid extent (EPSG:27700) using `ST_Hilbert(geom, BOX_2D)`. Sorting by this key clusters spatially adjacent features into consecutive row groups, maximising SedonaDB's ability to skip row groups during spatial joins.

**Fine-grained row groups** — USRNs use `row_group_size=20,000` (89 row groups across 1.76M rows); polygon datasets default to `10,000`. More row groups means more opportunities for SedonaDB to skip irrelevant data.

**GeoParquet 1.1 bbox covering columns** — A `bbox` struct column (`xmin`, `ymin`, `xmax`, `ymax`) is added to every row. Parquet writes min/max statistics on these floats into the file footer at the row group level. The geo metadata is patched with a `covering` key:

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

Two parquet optimisations are in play:

**Predicate pushdown** — The bbox covering columns enable row group skipping. SedonaDB checks the `xmin/ymin/xmax/ymax` min/max statistics in the file footer for each row group and skips any group whose bbox doesn't overlap the query. No WKB bytes are read for skipped row groups.

**Projection pushdown** — Because parquet is columnar, selecting only `usrn`, `street_type`, `geometry` and the chosen RHS columns means the reader fetches only those column chunks from disk. Every column not selected is never touched.

**ZSTD compression** — All columns compressed with ZSTD; low-cardinality string columns use `RLE_DICTIONARY` encoding automatically.

---

## Spatial phase (match)

**Two-phase spatial join (R-tree + refinement)**

SedonaDB executes each join in two phases:

1. **Index phase** — An R-tree built with Hilbert curve ordering finds candidate geometry pairs from their bounding rectangles, without touching WKB bytes.
2. **Refinement phase** — The exact spatial predicate (`ST_Intersects` or `ST_DWithin`) is evaluated only on candidates.

**Build/probe side assignment**

Sedona automatically assigns the smaller table to the build side (R-tree index) and the larger to the probe side, based on cardinality estimates (`should_swap_join_order` in `physical_planner.rs`). For stops (434K) vs USRNs (1.76M): stops = build, USRNs = probe.

**Speculative execution mode**

Sedona's default `execution_mode` is `Speculative(N)`. It samples the first N probe-side geometries at runtime and picks the best refinement strategy:

- `prepare_build` — lazily creates GEOS `PreparedGeometry` objects for build-side geometries on first use, caching them for reuse across all probe comparisons. Worth it for complex polygons.
- `prepare_probe` — prepares probe-side geometries instead. Better when the probe side has complex geometry.
- `prepare_none` — no prepared geometries. Optimal for simple geometry types like points.

In practice, Speculative sometimes chooses `prepare_none` (`execution_mode=0`) for point datasets (e.g. Naptan Nodes).

**Geometry clipping (intersect join)**

`ST_Intersection(u.geometry, s.geometry)` is used rather than returning full USRN geometries. A USRN crossing three polygons produces three rows, each with only the segment inside that polygon. When a bbox is supplied the result is also clipped to its boundary.

---

## DTF export phase

The DTF GeoParquet output uses the same DuckDB pipeline as the prepare phase. After building the DTF column layout, the shapely geometries are serialised to WKB and registered as a PyArrow table directly in DuckDB memory (no temp file).

DuckDB then computes the inline `bbox` struct, Hilbert-sorts the rows, and writes the GeoParquet file via `COPY TO PARQUET`. The same `_patch_covering_metadata` step upgrades the file to GeoParquet 1.1 with the covering key and CRS PROJJSON.

**DuckDB ↔ PyArrow zero-copy registration**

Both the sort and write steps use `con.register("name", arrow_table)` to hand an in-memory PyArrow table to DuckDB without copying or serialising it. DuckDB reads the Arrow columnar buffers in place and the registered name becomes a virtual table in any subsequent SQL:

```python
con = duckdb.connect()
con.execute("LOAD spatial;")
con.register("_dtf_src", arrow_table)   # pa.Table — no copy, no file

con.sql("SELECT ST_Hilbert(ST_GeomFromWKB(rhs_geometry), ...) FROM _dtf_src")
```

The geometry column is stored as `pa.binary()` (raw WKB bytes). DuckDB sees it as a `BLOB`, so `ST_GeomFromWKB()` deserialises it into DuckDB's internal `GEOMETRY` type on the fly. This means spatial functions (`ST_Hilbert`, `ST_XMin`, etc.) work directly on data that lives in Python memory, with no round-trip to disk.

This is DuckDB's "replacement scan" feature — registered Python objects (Arrow tables, Pandas DataFrames, NumPy arrays) are substituted into the query plan as virtual tables. It works because DuckDB and PyArrow share the Arrow columnar memory layout.
