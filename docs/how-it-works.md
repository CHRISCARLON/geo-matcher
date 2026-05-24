# How it works

## Prepare phase

Source files are converted to optimised GeoParquet 1.1 in `output_data/`. Run once.

**DuckDB pipeline** — `ST_Read()` ingests GeoPackage/shapefile sources; `read_csv()` + `ST_Point()` builds point geometries from CSV coordinate columns. DuckDB sorts, computes the bbox struct, and writes the Parquet file in a single `COPY TO PARQUET` statement. A PyArrow post-processing step patches the GeoParquet 1.1 covering metadata into the file footer.

**Hilbert sort** — Geometries are sorted by a Hilbert curve key computed from each feature's centroid within the BNG extent (`ST_Hilbert(geom, BOX_2D)`). This clusters spatially adjacent features into consecutive row groups, maximising row-group pruning during joins.

**bbox covering columns** — A `bbox` struct column (`xmin/ymin/xmax/ymax`) is written to every row. Parquet records min/max statistics for these floats in the file footer at the row-group level. The GeoParquet 1.1 `covering` key in the geo metadata points at these columns so both DuckDB and SedonaDB can skip row groups without reading any geometry bytes.

**Fine-grained row groups** — USRNs use `row_group_size=20,000` (89 row groups for 1.76M rows); other datasets default to `10,000`. More row groups means more pruning opportunities.

---

## Match phase

**Why bbox struct predicates, not `ST_Intersects`** — `ST_Intersects` is a geometry computation; DataFusion cannot push it to parquet row-group statistics. Scalar `bbox.xmin <= X` predicates ARE pushed to parquet column min/max statistics, skipping entire row groups before a single geometry byte is read.

**Filtered joins (bbox / city)** — Both the USRN and RHS parquets are registered as Sedona/DataFusion views. A single SQL query runs with bbox struct predicates on both `u.bbox.*` and `s.bbox.*` pushed down to parquet column statistics on both sides.

**National joins (no bbox)** — The RHS parquet is registered as a persistent Sedona view once. USRN row groups are split into batches via PyArrow. For each batch: the USRN rows are loaded into memory, the batch's spatial envelope is derived from the parquet row-group statistics, and that envelope is passed as the SQL `{spatial_filter}`. DataFusion prunes the persistent RHS scan to matching row groups at query time — no RHS data is ever loaded into Python. Each USRN appears in exactly one batch, so there are no duplicate (usrn, rhs) pairs.

**Join types:**

| Mode | Predicate | Use for |
|---|---|---|
| `intersect` | `ST_Intersects(u.geometry, s.geometry)` | Polygons, lines |
| `nearest` | `ST_DWithin(u.geometry, s.geometry, distance_m)` ordered by distance | Points |
| `line` | `ST_DWithin` + intersection-preference post-filter when `--rhs-id-col` is set | Linestrings |

**Geometry modes** — `--geometry none` (default) is attribute-only and fastest. `usrn` returns the USRN linestring; `clip` returns `ST_Intersection(usrn, rhs_polygon)`; `rhs` returns the matched RHS feature geometry (required for DTF export).

---

## DTF export phase

`_build_dtf_table` constructs a PyArrow table with type 70 fixed fields, RHS attribute columns, and a WKB geometry column. That table is registered directly in DuckDB memory via `con.register()` — no temp file, no copy (DuckDB reads Arrow columnar buffers in place via its replacement scan feature). DuckDB then computes inline bbox struct, Hilbert-sorts, and writes GeoParquet via `COPY TO PARQUET`. The same `_patch_covering_metadata` step upgrades the file to GeoParquet 1.1.
