# How it works

## Prepare phase

Source files are converted to optimised GeoParquet 1.1 in `output_data/`. Run once.

I'll soon account for GeoParquet 2.0.

**Sources** — `st_read()` ingests any GDAL-readable format (GeoPackage, Shapefile, …); `read_csv()` builds geometry from x/y columns for points or a WKT column for lines/polygons; `read_parquet()` re-sorts/re-compresses an existing GeoParquet, reprojecting via `ST_Transform(..., always_xy := true)` only when a foreign `source_crs` is given. For every other source the CRS is *asserted*, not reprojected — the file must already be EPSG:27700.

**One `COPY`, three jobs** — each source's `SELECT` is wrapped in a single `COPY ... TO PARQUET` statement (geometry materialised once in a subquery so it isn't recomputed): it adds a `bbox` struct (`ST_XMin/YMin/XMax/YMax`), Hilbert-sorts by `ST_Hilbert(geometry, BOX_2D)` within the BNG extent, and writes ZSTD-compressed Parquet. The Hilbert sort clusters spatially adjacent features into consecutive row groups; Parquet then records min/max stats on `bbox` per row group, and the GeoParquet 1.1 `covering` key (patched in by a PyArrow post-processing step — DuckDB itself only writes 1.0.0 metadata) points at those columns so a join can skip whole row groups without reading geometry bytes.

**Row group size** — plain USRN/UPRN prep (`prepare-usrns`/`prepare-uprns`) is fixed at 20,000 (89 row groups for USRN's 1.76M rows) and isn't configurable. Every other prepare command exposes `--row-group-size`/`--rhs-row-group-size`: the line/buffer variants default to 20,000, `prepare-gpkg`/`prepare-csv`/`prepare-parquet` to 10,000 — see the `--lhs-name uprn` note further down for when to lower it.

**UPRN prep** (`prepare-uprns`/`prepare-uprns-buffer`) mirrors USRN's plain/buffered split, with two changes for its scale (~41.6M rows vs. USRN's 1.76M): the source's uppercase `UPRN` column is renamed to lowercase `uprn`, and only `uprn` + `geometry` are kept — `x/y/lat/lon` are dropped as redundant with `geometry`. The buffered file has the same `geometry`/`geometry_point` split USRN's buffered file has for `geometry`/`geometry_line` (`geometry` becomes `ST_Buffer(point, buffer_m)`; the original point survives as `geometry_point`). No new join code needed — `uprns_27700.parquet` is an ordinary RHS point dataset via `--mode point`.

---

## Match phase

Output is attribute-only — no geometry column. Results are a plain tabular join of USRNs to RHS dataset attributes.

**Filtered joins (bbox / city)** — Both the USRN and RHS parquets are registered as Sedona/DataFusion views. A single SQL query runs with `ST_Intersects` predicates against the bbox polygon in the WHERE clause; Sedona skips non-overlapping row groups on both sides via GeoParquet 1.1 covering metadata.

**National joins (no bbox)** — The USRN parquet is registered as a Sedona view once (metadata only — no rows loaded). The RHS parquet is split into `--batches` in-memory slices via PyArrow. For each slice, its spatial envelope is derived from the Parquet footer row-group statistics (no geometry read) and injected as an `ST_Intersects` predicate into the SQL. Sedona uses the USRN GeoParquet 1.1 covering metadata to skip row groups that don't overlap that envelope — so only the USRN row groups that spatially overlap the current RHS slice are read from disk and joined. Results are written incrementally to a `ParquetWriter` — at most one slice's matched rows are in memory at a time.

**Which side actually gets a SQL spatial filter.** In national mode, the RHS is never given a `WHERE`-clause spatial predicate at all — it's already bounded a cheaper way, by reading only the parquet row groups that belong to the current slice (chosen from `bbox` covering-column statistics, no geometry read). Only the USRN side gets the `ST_Intersects(u.geometry, envelope)` predicate, pruning USRN row groups against that slice's envelope. In filtered mode there's no chunking, so the bbox has to do that work itself as a `WHERE` predicate on both sides — see below for how that predicate differs by side.

**Exact bbox vs. expanded bbox — why `u` and `s` aren't pruned the same way.** Whether the RHS side of a filter is pruned to the *exact* bbox or one *expanded* by the match distance depends on the predicate:

- `ST_Intersects` - style joins (`bbox_pruner`, used by the polygon join and line-join Phase 1) prune **both** sides to the exact bbox. A match here requires actual geometric overlap — a RHS feature entirely outside the bbox cannot intersect a USRN inside it without itself crossing into the bbox, in which case it's still caught by the exact-bbox predicate. Pruning both sides exactly loses nothing.

- `ST_DWithin` - style joins (`bbox_nearest_filters`, used by the point join; and the line-join Phase 3/4 batch filters) require only *proximity*, not overlap. A real match can be an RHS feature sitting just outside the bbox but within `distance_m` of a USRN just inside it. Pruning RHS to the exact bbox would silently drop that as a false negative purely from where the tile boundary fell — not from the data. So the non-anchor side is expanded by the match radius (`distance_m` for the point join, `phase4_tolerance_m` for Phase 4) to guarantee no true match is missed, while still avoiding an unbounded scan of the RHS dataset. The USRN side stays exact because USRNs outside the bbox aren't wanted in the output — only RHS features that might match one inside it are.

The same rule runs the other way in the line-join Phase 3/Phase 4 batch loops: there the RHS batch is the fixed anchor, so it's the USRN filter that gets expanded outward by the match radius instead.

**Join modes:**

| Lhs | Mode | Architecture | Predicate | Use for |
|---|---|---|---|---|
| `usrn` (default) | `polygon` | Direct spatial join · 1 USRN file | `ST_Intersects(u.geometry, s.geometry)` | Polygons, areas |
| `usrn` | `point` | Direct spatial join · 1 USRN file | `ST_DWithin(u.geometry, s.geometry, distance_m)` ordered by distance | Points |
| `usrn` | `line` | USRN line-network match · 2 USRN files | Phases 1+2: `ST_Intersects` then corridor, both over every feature; Phase 3: nearest fallback; Phase 4: connectivity inheritance | Linestrings |
| `uprn` | `polygon` | Direct spatial join · 1 UPRN file | `ST_Intersects(u.geometry, s.geometry)` | Polygons, areas |

The join registry is keyed by `(lhs, mode)` — `--lhs-name` picks the base dataset to
join *from* (`usrn` street centrelines by default, or `uprn` address points),
`--mode` picks the RHS geometry strategy. Not every `(lhs, mode)` combination is
registered — currently `uprn` only has a `polygon` join — `geo-matcher match`
raises a clear error listing the registered combinations if you ask for one
that doesn't exist. The `uprn` polygon join reuses the exact same direct-spatial-join
engine (`execute_join`) as the `usrn` polygon join; it just registers the UPRN
GeoParquet as the `uprns` Sedona view instead of `usrns`.

**All four rows in the table above go through one `execute_join` entry point**,
which dispatches on two independent axes:

1. **Kind of join** — decided by which keyword arguments the caller passes, not
   by an explicit flag. `query` + `filter_fn` means a direct spatial join
   (`polygon`/`point`); `line_phases` means the USRN line-network match. Passing
   both, or an incomplete pair, raises immediately rather than guessing.
2. **Mode** — a `FilteredMode`/`NationalMode` match that runs separately inside
   whichever kind was selected, since each kind calls different executor
   functions: `_filtered_spatial_join`/`_national_spatial_join` for a direct
   spatial join, `_filtered_line_join`/`_national_line_join` for the line match.

Four executors, reached by a 2×2 of (kind, mode) — no per-dataset-type
branching lives outside `execute_join` itself.

**`--lhs-name uprn` needs a finely-row-grouped RHS file.** `_split_into_chunks` can
only split the RHS at existing row-group boundaries — it can never subdivide a
row group further — so the RHS file's row-group count sets a hard ceiling on
how many chunks a national join can ever have, no matter what `--batches` is
passed. That ceiling barely matters for `usrn` joins (1.76M LHS rows), but with
`uprn` as the LHS (41.6M rows, 23x more), a coarsely-row-grouped RHS produces
chunks with such broad spatial envelopes that a huge fraction of UPRN rows
become join candidates per chunk. Concretely: matching `uprn` against a `soil`
RHS prepared at the default `row_group_size=10,000` (42,603 rows → only 5 row
groups) OOM'd/hung after chunk 1 (5.5M matches from that one chunk alone).
Re-preparing `soil` with `--rhs-row-group-size 100` (→ 427 row groups) and
matching with `--batches 200` (→ 143 actual chunks, ~3 row groups/~300 rows
each) fixed it: the full national join completed in ~60s, streaming
37.7M matches with no memory blowup. When adding a new RHS dataset for use
with `--lhs-name uprn`, prepare it with a small `--rhs-row-group-size` (or
`--row-group-size` for `prepare-csv`/`prepare-parquet`) and pass a `--batches`
high enough to actually spend that row-group count as chunks — see
`prepare-soil`/`match-soil-uprn` in the `Makefile` for a working example.

**USRN line-network match strategy**

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
a main asset. `distance_m` still reports the true distance to the inherited centreline, so a
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
