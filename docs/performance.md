# Performance internals

A plain-language explainer for the terms and mechanisms behind national-join
performance — row groups, chunks, batches, and the read-ahead prefetch.

---

## Row groups vs. chunks vs. batches

Three different things, each controlled by a different flag, which is easy to confuse sometimes:

- **Row groups** — a property of the Parquet file itself: how many physical
  blocks it got split into when it was *prepared*.
- Set once, at prepare time, via `--row-group-size`/`--rhs-row-group-size`.
- Fixed after that — the join can't change it, only read what's there.
- **Chunks** — how many pieces the *join* splits the RHS into while running.
- Controlled by `--batches` (it really means "how many chunks", not a row count).
- A chunk is built from whole row groups; it can never be smaller than one.
- So chunk count is always `min(--batches, row group count)` — asking for
  more `--batches` than there are row groups just clamps down to one chunk
  per row group.
- Only national (no-bbox) joins chunk at all — filtered/bbox joins process
  the RHS in one go.
- **Batches** (`--rows-per-batch`) — a completely separate, row-*count*-based
  split, used only inside line-join Phase 3/4, splitting whatever's still
  unmatched after Phases 1/2 into fixed-size groups (default 5,000 rows).
- Has nothing to do with row groups or chunks, and doesn't apply to
  `--mode polygon`/`--mode point` joins at all.

**Worked example:** `stops_27700.parquet` prepared at `--row-group-size 1000`
against 434,417 rows gives 435 row groups. Running
`geo-matcher match --rhs-name stops --mode point` with the default
`--batches 100` produces `min(100, 435) = 100` chunks. If it had instead been
`--mode line`, each chunk's still-unmatched features after Phase 1/2 would
further split into `--rows-per-batch`-sized batches for Phase 3/4 — a fourth,
independent split layered on top.

---

## Read-ahead prefetch

National joins read one RHS chunk, run a Sedona query against it, write the
result, then move to the next chunk. `_prefetch` (`join.py`) overlaps the
*next* chunk's read with the *current* chunk's query.

- The fetcher = a background thread reading the next chunk's row groups off
  disk (`_read_rhs_chunks`).
- The tray = a `queue.Queue(maxsize=1)` — the one-slot hand-off point.
- The worker = the main join loop, running `_register_rhs_view` + the
  Sedona query for whichever chunk it currently has.
- Because reading a chunk (a few ms) is much cheaper than querying it
  (hundreds of ms), the read finishes and sits ready well before it's
  needed — the query cost fully hides the read cost.
- Peak memory goes from "1 chunk" to "at most 2 chunks" (one being queried,
  one waiting on the tray) — never more, and the Sedona queries themselves
  always run one at a time, never in parallel.
- Wrapping this in a generator (`_prefetch` yields items like any other
  iterable) is just packaging — it lets the calling `for` loop stay
  unchanged. The actual overlap comes entirely from the background thread;
  a generator alone, with no thread, would give zero speedup.

Set `GEO_MATCHER_DEBUG_LEVEL=DEBUG` (the default) and watch for
`_prefetch +Ns: ...` log lines to see this play out on a real run — each
chunk's `read_ahead enqueued item N` should land while the previous chunk's
`INFO Chunk N/M: ... matches` line is still pending.
