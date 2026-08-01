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
| `line` | Four-phase · 2 USRN files | Phases 1+2: `ST_Intersects` then corridor, both over every feature; Phase 3: nearest fallback; Phase 4: connectivity inheritance | Linestrings |

**Line join four-phase strategy**

Phases 1 and 2 both run over **every** feature in the slice and their results are unioned.
They answer different questions — "does this line cross a street?" and "does this line run
along one?" — and a line can legitimately do both, to different streets. 

Gating Phase 2 on Phase 1's leftovers used to make that impossible: a line that clipped one street's
centreline could never be associated with the street it ran alongside for its whole length.

Where a feature crosses a centreline *and* overlaps that same street's corridor, only the
Phase 1 row is kept — the pair is already reported with stronger evidence, and letting the
near-total self-overlap into the corridor ranking would suppress every genuinely adjacent
street.

Phases 3 and 4 remain strict fallbacks: Phase 3 sees only what neither Phase 1 nor Phase 2
matched, and Phase 4 only what Phase 3 also missed.

---

**Phase 1 — Direct intersection** (`is_intersection=true`, `match_phase=1`)

The RHS line and the USRN centreline actually touch. Definitive match — every touching
pair is kept, with no overlap threshold and no ranking. A line crossing five streets gets
all five.

```
                          │
                          │   RHS line
                          │
USRN centreline  ─────────┼─────────────
                          │
                          │   ...crosses straight through
                          │
```

Predicate: `ST_Intersects(usrn.geometry, rhs.geometry)` — matched against the raw
centrelines in `usrns_27700.parquet`, not the buffered corridors.

**What Phase 1 hands to Phase 2.** Phase 1 no longer decides which features Phase 2 gets
to look at — Phase 2 re-reads the whole slice regardless. What it hands over is the list
of `(feature, usrn)` **pairs** it produced, and Phase 2 uses that only to subtract those
exact pairs from its own candidates.

That distinction is the whole point. A crossed street sits at distance 0 from the line, so
its corridor covers a large share of the line's length and it would score near the top of
Phase 2's ranking — where the rule "keep every corridor within 80 % of the best" would
then discard the streets the line genuinely runs along. Removing the pair before scoring
means the ranking compares adjacent streets against each other, not against a street the
line merely clipped:

```
before, gated on Phase 1:      after, pair-level exclusion:

street A  overlap 1.00  ← kept  street A  (already Phase 1 — removed before scoring)
street B  overlap 0.50  ← cut   street B  overlap 0.50  ← now the best, kept
street C  overlap 0.45  ← cut   street C  overlap 0.45  ← within 80 % of 0.50, kept
```

It also keeps the output honest: street A is reported once, as `match_phase=1`, rather
than appearing a second time as a corridor row.

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
Post-filter: `overlap_length_pct >= --overlap-threshold` (default 10 %), then every
corridor within 80 % of the feature's best, so a line straddling two streets gets both.
`(feature, usrn)` pairs already returned by Phase 1 are removed before that ranking.

Because this runs over every feature, a line can hold both a Phase 1 row for the street
it crosses and a Phase 2 row for the street it runs along:

```
street A         ───────────────────────  (Phase 1 — crossed)
                          │
street B buffer  ▓▓▓▓▓▓▓▓▓┼▓▓▓▓▓▓▓▓▓▓▓▓
street B         ─────────┼─────────────  (Phase 2 — run alongside)
                  ════════╪════════       RHS line
```

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

---

**Phase 4 — Connectivity inheritance** (`is_intersection=false`, `overlap_length_pct=0.0`, `match_phase=4`)

The RHS line never comes within `--phase3-distance` of any street, but physically touches
a feature that did. Rather than reaching for an ever-more-distant street, it inherits that
neighbour's USRN — a claim about network membership, not proximity. Typical for spurs off
a main run. `distance_m` still reports the true distance to the inherited centreline, so a
consumer can see how far the attribution reaches; it is usually well beyond
`--phase3-distance`, which is exactly why these rows are flagged separately.

```
street           ───────────────────────
                          ════            already-matched main (Phase 1/2/3)
                            ╲
                             ╲  ← within --phase4-tolerance (default 5 m)
                              ══          RHS spur, inherits the main's USRN
```

Predicate: `ST_DWithin(neighbour.geometry, rhs.geometry, phase4_tolerance_m)`  
Dedup: one row per RHS feature (closest neighbour's USRN wins). Disable with
`--phase4-tolerance 0`.
