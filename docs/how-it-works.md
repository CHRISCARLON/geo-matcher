# How it works

## Prepare phase

Source files are converted to optimised GeoParquet 1.1 in `output_data/`. Run once.

I'll soon account for GeoParquet 2.0.

**Sources:**
- `st_read()` ingests any GDAL-readable format (GeoPackage, Shapefile, …).
- `read_csv()` builds geometry from x/y columns (points) or a WKT column (lines/polygons).
- `read_parquet()` re-sorts/re-compresses an existing GeoParquet.
- Reprojection (`ST_Transform(..., always_xy := true)`) only happens when a foreign `source_crs` is given.
- Otherwise the CRS is *asserted*, not reprojected — the file must already be EPSG:27700.

**Write pipeline** (one `COPY ... TO PARQUET` per source, geometry materialised once in a subquery):
- Adds a `bbox` struct (`ST_XMin/YMin/XMax/YMax`).
- Hilbert-sorts by `ST_Hilbert(geometry, BOX_2D)` within the BNG extent — clusters spatially adjacent features into consecutive row groups.
- Writes ZSTD-compressed Parquet.
- Patches in the GeoParquet 1.1 `covering` key (a PyArrow post-processing step — DuckDB itself only writes 1.0.0 metadata).
- That covering key is what lets a join skip whole row groups without reading geometry bytes.

**Row group size:**
- `prepare-usrns`/`prepare-uprns` are fixed at 20,000 (89 row groups for USRN's 1.76M rows), not configurable.
- `prepare-usrns-line`/`prepare-uprns-buffer` default to 20,000 via `--row-group-size`.
- `prepare-gpkg`/`prepare-csv`/`prepare-parquet` default to 10,000 via `--row-group-size`/`--rhs-row-group-size`.
- See the `--lhs-name uprn` note further down for when to lower it.

**UPRN prep** (`prepare-uprns`/`prepare-uprns-buffer`):
- Mirrors USRN's plain/buffered split, scaled for ~41.6M rows (23x USRN's 1.76M).
- Renames the source's uppercase `UPRN` column to lowercase `uprn`.
- Keeps only `uprn` + `geometry` — `x/y/lat/lon` dropped as redundant with `geometry`.
- Buffered file splits `geometry`/`geometry_point`, the same pattern USRN's buffered file uses for `geometry`/`geometry_line`.
- No new join code needed — `uprns_27700.parquet` is an ordinary RHS point dataset via `--mode point`.

---

## Match phase

Output is attribute-only — no geometry column. Results are a plain tabular join of USRNs to RHS dataset attributes.

**Filtered joins (bbox / city):**
- USRN and RHS parquets both registered as Sedona/DataFusion views.
- One SQL query runs, with `ST_Intersects` predicates against the bbox in the WHERE clause.
- Sedona skips non-overlapping row groups on both sides via GeoParquet 1.1 covering metadata.

**National joins (no bbox):**
- USRN parquet registered as a Sedona view once — metadata only, no rows loaded.
- RHS parquet split into `min(--batches, row_groups)` in-memory slices (`_split_into_chunks`), sized as evenly as possible.
- An even split, not a fixed stride — e.g. `--batches 200` against a 427-row-group file yields 200 real chunks of 2-3 row groups each, not a stride-rounded 143.
- Each slice's spatial envelope is derived from Parquet footer row-group stats (no geometry read) and injected as an `ST_Intersects` predicate.
- Sedona uses USRN's covering metadata to skip row groups that don't overlap that envelope.
- Results are written incrementally to a `ParquetWriter` — at most one slice's matched rows in memory at a time.
- The *next* slice's read (`_prefetch`) runs on a background thread while the current slice's query executes.
- Prefetch is bounded to exactly one slice of read-ahead — peak memory goes from "1 slice" to "at most 2 slices", never unbounded.
- The Sedona queries themselves stay strictly sequential — only the I/O-bound read is overlapped.

**Which side gets a SQL spatial filter:**
- National mode: the RHS is never given a WHERE-clause predicate — it's bounded a cheaper way, by which row groups get read for the current slice.
- National mode: only the USRN side gets `ST_Intersects(u.geometry, envelope)`, pruning USRN row groups against that slice's envelope.
- Filtered mode: there's no chunking, so the bbox is applied as a WHERE predicate on both sides instead.

**Exact bbox vs. expanded bbox — why `u` and `s` aren't pruned the same way:**
- `ST_Intersects` joins (`bbox_pruner`: polygon join, line-join Phase 1) prune **both** sides to the exact bbox — a match requires actual overlap, so pruning both sides exactly loses nothing.
- `ST_DWithin` joins (`bbox_nearest_filters`: point join, line-join Phase 3/4 batch filters) require only proximity, not overlap.
- A real match can sit just outside the bbox but within `distance_m` of a USRN just inside it, so the non-anchor side is expanded by the match radius (`distance_m` for point, `phase4_tolerance_m` for Phase 4) to avoid a false negative from where the tile boundary fell.
- The USRN side always stays exact — USRNs outside the bbox aren't wanted in the output.
- The rule runs the other way in the line-join Phase 3/4 batch loops: there the RHS batch is the anchor, so it's the USRN filter that gets expanded instead.

**Join modes:**

| Lhs | Mode | Architecture | Predicate | Use for |
|---|---|---|---|---|
| `usrn` (default) | `polygon` | Direct spatial join · 1 USRN file | `ST_Intersects(u.geometry, s.geometry)` | Polygons, areas |
| `usrn` | `point` | Direct spatial join · 1 USRN file | `ST_DWithin(u.geometry, s.geometry, distance_m)` ordered by distance | Points |
| `usrn` | `line` | USRN line-network match · 2 USRN files | Phases 1+2: `ST_Intersects` then corridor, both over every feature; Phase 3: nearest fallback; Phase 4: connectivity inheritance | Linestrings |
| `uprn` | `polygon` | Direct spatial join · 1 UPRN file | `ST_Intersects(u.geometry, s.geometry)` | Polygons, areas |

- The join registry is keyed by `(lhs, mode)` — `--lhs-name` picks the base dataset to join *from*, `--mode` picks the RHS geometry strategy.
- Not every `(lhs, mode)` combination is registered — currently `uprn` only has `polygon`.
- `geo-matcher match` raises a clear error listing the registered combinations if you ask for one that doesn't exist.
- The `uprn` polygon join reuses the exact same direct-spatial-join engine (`execute_join`) as the `usrn` polygon join, just registering `uprns` as the Sedona view instead of `usrns`.

**`execute_join` — single entry point for every registered join**, dispatches on two independent axes:
1. Kind of join — decided by which keyword arguments the caller passes, not an explicit flag: `query` + `filter_fn` means a direct spatial join (`polygon`/`point`), `line_phases` means the USRN line-network match.
2. Passing both, or an incomplete pair, raises immediately rather than guessing.
3. Mode — a `FilteredMode`/`NationalMode` match that runs separately inside whichever kind was selected, via a different executor function each.
4. Four executors total, reached by a 2×2 of (kind, mode) — no per-dataset-type branching lives outside `execute_join` itself.

**`--lhs-name uprn` needs a finely-row-grouped RHS file:**
- `_split_into_chunks` can only split the RHS at existing row-group boundaries — it can never subdivide a row group further.
- The RHS file's row-group count sets a hard ceiling on how many chunks a national join can ever have.
- `--batches` past that ceiling is harmless — it now clamps to `min(--batches, row_groups)` instead of undershooting.
- That ceiling barely matters for `usrn` joins (1.76M LHS rows).
- It matters a lot for `uprn` (41.6M rows, 23x more) — a coarsely-row-grouped RHS gives chunks such broad spatial envelopes that a huge fraction of UPRN rows become join candidates per chunk.
- Case study: `uprn` × `soil` prepared at default `row_group_size=10,000` (5 row groups) OOM'd/hung after chunk 1 — 5.5M matches from that one chunk alone.
- Fix: re-prepare `soil` with `--rhs-row-group-size 100` (427 row groups), match with `--batches 200` (200 actual chunks, ~2-3 row groups/~200-300 rows each).
- Result: the full national join completed in ~60s, streaming 37.7M matches with no memory blowup.
- For a new RHS dataset used with `--lhs-name uprn`: prepare it with a small `--rhs-row-group-size`/`--row-group-size`, and pass a `--batches` high enough to actually spend that row-group count as chunks.
- The default `--batches` is 100 — already generous for smaller files (it just clamps down) but may still need raising for a very finely-grouped one.
- See `prepare-soil`/`match-soil-uprn` in the `Makefile` for a working example.

**USRN line-network match strategy:**
- Phases 1 and 2 both run over **every** feature and their results are unioned.
- They answer different questions — "does this line cross a street?" vs. "does this line run along one?" — and a line can legitimately do both, to different streets.
- Gating Phase 2 on Phase 1's leftovers used to make that impossible: a line clipping one street's centreline could never be associated with a street it ran alongside for its whole length.
- Where a feature crosses a centreline *and* overlaps that same street's corridor, only the Phase 1 row is kept.
- That pair is already reported with stronger evidence, and letting the near-total self-overlap into the corridor ranking would suppress every genuinely adjacent street.
- Phases 3 and 4 remain strict fallbacks: Phase 3 sees only what neither Phase 1 nor Phase 2 matched, Phase 4 only what Phase 3 also missed.

---

**Phase 1 — Direct intersection** (`is_intersection=true`, `match_phase=1`)

- The RHS line and the USRN centreline actually touch.
- Definitive match — every touching pair is kept, no overlap threshold, no ranking.
- A line crossing five streets gets all five.

```
                          │
                          │   RHS line
                          │
USRN centreline  ─────────┼─────────────
                          │
                          │   ...crosses straight through
                          │
```

- Predicate: `ST_Intersects(usrn.geometry, rhs.geometry)` — against the raw centrelines in `usrns_27700.parquet`, not the buffered corridors.
- Phase 1 no longer decides which features Phase 2 gets to look at — Phase 2 re-reads the whole slice regardless.
- What Phase 1 hands over is the list of `(feature, usrn)` **pairs** it produced, used only to subtract those exact pairs from Phase 2's candidates.
- That distinction is the whole point: a crossed street sits at distance 0, so its corridor covers a large share of the line and would rank near the top of Phase 2's "keep every corridor within 80% of the best" window — suppressing the streets the line genuinely runs along.
- Removing the pair before scoring means the ranking compares adjacent streets against each other, not against a street the line merely clipped:

```
before, gated on Phase 1:      after, pair-level exclusion:

street A  overlap 1.00  ← kept  street A  (already Phase 1 — removed before scoring)
street B  overlap 0.50  ← cut   street B  overlap 0.50  ← now the best, kept
street C  overlap 0.45  ← cut   street C  overlap 0.45  ← within 80 % of 0.50, kept
```

- It also keeps the output honest: street A is reported once, as `match_phase=1`, rather than a second time as a corridor row.

---

**Phase 2 — Corridor match** (`is_intersection=false`, `match_phase=2`)

- The RHS line runs alongside the USRN without crossing it, with at least 10% of its length inside the USRN's buffer corridor.
- Typical for pipes or cables running under a pavement parallel to the road.

```
USRN centreline  ───────────────────────
buffer           ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
                   ═══════════════════   RHS line (parallel, inside buffer)
```

- Predicate: `ST_Intersects(usrn_corridor.geometry, rhs.geometry)`.
- Post-filter: `overlap_length_pct >= --overlap-threshold` (default 10%), then every corridor within 80% of the feature's best — a line straddling two streets gets both.
- `(feature, usrn)` pairs already returned by Phase 1 are removed before that ranking.
- Because this runs over every feature, a line can hold both a Phase 1 row (the street it crosses) and a Phase 2 row (the street it runs along):

```
street A         ───────────────────────  (Phase 1 — crossed)
                          │
street B buffer  ▓▓▓▓▓▓▓▓▓┼▓▓▓▓▓▓▓▓▓▓▓▓
street B         ─────────┼─────────────  (Phase 2 — run alongside)
                  ════════╪════════       RHS line
```

---

**Phase 3 — Nearest fallback** (`is_intersection=false`, `overlap_length_pct=0.0`, `match_phase=3`)

- The RHS line didn't intersect any centreline or corridor.
- The single closest USRN within `--phase3-distance` metres is assigned.
- Catches short stubs and diagonal mains that fall just outside the corridor threshold.

```
USRN centreline  ───────────────────────
                  ·  ·  ·  ·  ·  ·        (within phase3-distance)
                          ════            RHS line (no corridor overlap)
```

- Predicate: `ST_DWithin(usrn.geometry, rhs.geometry, phase3_distance_m)`.
- Dedup: one row per RHS feature (closest USRN wins).

---

**Phase 4 — Connectivity inheritance** (`is_intersection=false`, `overlap_length_pct=0.0`, `match_phase=4`)

- The RHS line never comes within `--phase3-distance` of any street, but physically touches a feature that did.
- It inherits that neighbour's USRN — a claim about network membership, not proximity.
- Typical for spurs off a main asset.
- `distance_m` still reports the true distance to the inherited centreline — usually well beyond `--phase3-distance`, which is why these rows are flagged separately.

```
street           ───────────────────────
                          ════            already-matched main (Phase 1/2/3)
                            ╲
                             ╲  ← within --phase4-tolerance (default 5 m)
                              ══          RHS spur, inherits the main's USRN
```

- Predicate: `ST_DWithin(neighbour.geometry, rhs.geometry, phase4_tolerance_m)`.
- Dedup: one row per RHS feature (closest neighbour's USRN wins).
- Disable with `--phase4-tolerance 0`.
