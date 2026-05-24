# How it works

## Prepare phase

Source files are converted to optimised GeoParquet 1.1 in `output_data/`. Run once.

**DuckDB pipeline** — `ST_Read()` ingests GeoPackage/shapefile sources; `read_csv()` + `ST_Point()` builds point geometries from CSV coordinate columns. DuckDB sorts, computes the bbox struct, and writes the Parquet file in a single `COPY TO PARQUET` statement. A PyArrow post-processing step patches the GeoParquet 1.1 covering metadata into the file footer.

**Hilbert sort** — Geometries are sorted by a Hilbert curve key computed from each feature's centroid within the BNG extent (`ST_Hilbert(geom, BOX_2D)`). This clusters spatially adjacent features into consecutive row groups, maximising row-group pruning during joins.

**bbox covering columns** — A `bbox` struct column (`xmin/ymin/xmax/ymax`) is written to every row. Parquet records min/max statistics for these floats in the file footer at the row-group level. The GeoParquet 1.1 `covering` key in the geo metadata points at these columns so both DuckDB and SedonaDB can skip row groups without reading any geometry bytes.

**Fine-grained row groups** — USRNs use `row_group_size=20,000` (89 row groups for 1.76M rows); other datasets default to `10,000`. More row groups means more pruning opportunities.

---

## Match phase

Output is attribute-only — no geometry column. Results are a plain tabular join of USRNs to RHS dataset attributes.

**Filtered joins (bbox / city)** — Both the USRN and RHS parquets are registered as Sedona/DataFusion views. A single SQL query runs with `ST_Intersects` predicates against the bbox polygon in the WHERE clause; Sedona skips non-overlapping row groups on both sides via GeoParquet 1.1 covering metadata.

**National joins (no bbox)** — The USRN parquet is registered as a Sedona view once (metadata only — no rows loaded). The RHS parquet is split into `--batches` in-memory slices via PyArrow. For each slice, its spatial envelope is derived from the Parquet footer row-group statistics (no geometry read) and injected as an `ST_Intersects` predicate into the SQL. Sedona uses the USRN GeoParquet 1.1 covering metadata to skip row groups that don't overlap that envelope — so only the USRN row groups that spatially overlap the current RHS slice are read from disk and joined. Results are written incrementally to a `ParquetWriter` — at most one slice's matched rows are in memory at a time.

**Join modes:**

| Mode | Architecture | Predicate | Use for |
|---|---|---|---|
| `polygon` | Single-phase · 1 USRN file | `ST_Intersects(u.geometry, s.geometry)` | Polygons, areas |
| `point` | Single-phase · 1 USRN file | `ST_DWithin(u.geometry, s.geometry, distance_m)` ordered by distance | Points |
| `line` | Three-phase · 2 USRN files | Phase 1: `ST_Intersects`; Phase 2: corridor; Phase 3: nearest fallback | Linestrings |

**Line join three-phase strategy**

Each phase only processes features that were not matched by the previous phase.

---

**Phase 1 — Direct intersection** (`is_intersection=true`, `match_phase=1`)

The RHS line crosses the USRN centreline. Definitive match — kept unconditionally.

```
USRN centreline  ───────────────────────
                          │
                          │  RHS line crosses
                          │
```

Predicate: `ST_Intersects(usrn.geometry, rhs.geometry)`

---

**Phase 2 — Corridor match** (`is_intersection=false`, `match_phase=2`)

The RHS line runs alongside the USRN without crossing it, but at least 10 % of the
line's length falls inside the USRN's buffer corridor. Typical for pipes or cables
running under a pavement parallel to the road.

```
USRN centreline  ───────────────────────
buffer           ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
                   ═══════════════════   RHS line (parallel, inside buffer)
```

Predicate: `ST_Intersects(usrn_corridor.geometry, rhs.geometry)`  
Post-filter: `overlap_length_pct >= --overlap-threshold` (default 10 %)

---

**Phase 3 — Nearest fallback** (`is_intersection=false`, `overlap_length_pct=0.0`, `match_phase=3`)

The RHS line didn't intersect any centreline or corridor. The single closest USRN
within `--phase3-distance` metres is assigned. Catches short stubs and diagonal mains
that fall just outside the corridor threshold.

```
USRN centreline  ───────────────────────
                  ·  ·  ·  ·  ·  ·        (within phase3-distance)
                          ════            RHS line (no corridor overlap)
```

Predicate: `ST_DWithin(usrn.geometry, rhs.geometry, phase3_distance_m)`  
Dedup: one row per RHS feature (closest USRN wins)
