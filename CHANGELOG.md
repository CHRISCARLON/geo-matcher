# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4] - 2026-09-15

### Fixed

- `prepare()` on a large OGR source (GeoPackage, shapefile) no longer drives
  GDAL from inside the Hilbert-sort query. `_prepare_ogr` now stages the
  `st_read` to a plain Parquet file first — a straight scan, no sort, no join,
  geometry crossing as WKB — and the sort then runs from that staged file
  before it is deleted. GDAL's GeoPackage driver sits on SQLite and is not
  reliably thread-safe, and reading it from within a large parallel sort
  segfaulted the process (exit 139) on national datasets such as the
  1.77M-feature OS Open USRN GeoPackage on a CI runner. Output is unchanged:
  same rows, same columns, same geometry, and the `bbox` covering column still
  survives for row-group pruning.
- `prepare()` no longer leaks its DuckDB database. Every `_prepare_*` function
  opened a connection via `duckdb.connect()` and never closed it, so a process
  calling `prepare()` several times accumulated one live in-memory instance per
  call for its whole lifetime. All of them now go through a new `_connection()`
  context manager that closes on the way out, including on failure. This
  matters more than it looks: each `duckdb.connect()` is an *independent*
  instance that defaults `memory_limit` to ~80% of system RAM, so several live
  instances each believed they could use most of the machine.

### Added

- `prepare(..., memory_limit=...)` — an optional DuckDB `memory_limit` for the
  call, e.g. `"3GB"`, threaded through every `_prepare_*` function. There was
  previously no way for a caller to bound DuckDB's memory at all; on a
  memory-constrained runner the default (~80% of RAM, per instance) is too
  generous.
- `pyproj` declared as an explicit dependency. `prepare.py` imports it directly
  to write CRS PROJJSON into the GeoParquet metadata, but it was only arriving
  transitively via `apache-sedona` → `geopandas`, so a change in that chain
  would have broken `prepare()` at import.

### Changed

- `sedonadb` is no longer imported when `geo_matcher` is imported. `import
  sedona.db` moved from `matcher.py`'s module scope into `GeoMatcher._connect()`,
  and the annotation-only `SedonaContext` imports in `join.py` and `explain.py`
  moved under `TYPE_CHECKING` (both modules gained
  `from __future__ import annotations`). Callers that only ever call `prepare()`
  no longer pay to load Sedona's native library, which keeps one fewer native
  geo stack resident while GDAL is being driven hard.
- `pyogrio` is no longer used. `_prepare_ogr` read layer metadata (CRS, feature
  count, geometry type) via `pyogrio.read_info()`, which opened the source
  through a *second* bundled GDAL alongside duckdb-spatial's. That is now a
  `st_read_meta()` query on the connection already in hand, via the new
  `_read_ogr_info()`, keeping the prepare path on a single GDAL. `pyogrio` was
  also an undeclared dependency.

## [0.1.3] - 2026-09-01

### Changed

- `join.py`'s two join dispatchers merged into one: `execute_join` now handles
  every registered strategy (polygon/point USRN joins, the UPRN polygon join,
  and the USRN line-network match), dispatching on which keyword arguments are
  passed (`query`+`filter_fn` vs. `line_phases`) and then on `JoinMode`
  (`FilteredMode`/`NationalMode`) inside each. `execute_line_join` is gone from
  the public API; `LineJoinPhases` (a new frozen dataclass bundling the line
  match's SQL templates and parameters) is exported in its place.
- Renamed `_national_single_phase`/`_filtered_single_phase` to
  `_national_spatial_join`/`_filtered_spatial_join` — the old names described
  step count, not intent; these run one direct `ST_Intersects`/`ST_DWithin`
  predicate between LHS and RHS. Docstrings, comments, and log messages across
  `join.py` and `docs/how-it-works.md` reworded to lead with what each join
  does rather than its phase count.
- Extracted `_materialise_national` — the "stream to `output_path` if given,
  else stream to a scratch tempfile and read it back" logic that both
  `NationalMode` branches previously duplicated.

## [0.1.2] - 2026-08-31

### Changed

- `join.py`'s four-phase line-join ID bookkeeping (`_distinct_ids` and the
  matched/unmatched split in `_phase4_match`) moved off Python `set`s onto
  Arrow arrays throughout, so per-chunk match-phase id tracking stays inside
  PyArrow's compute layer (`pc.unique`, plus new `_union_ids`/`_intersect_len`
  helpers) instead of boxing every id to a Python object.
- Added `assets/geomatcher-mark-light.png` / `-dark.png` logo, shown at the
  top of the README via a theme-aware `<picture>` element.
- Added CI (`.github/workflows/ci.yml`): ruff, mypy, and pytest across
  Python 3.11–3.13 via `uv`.
- Added README badges (CI, licence, version, Python).
- Corrected `pyproject.toml`'s `license` field from `MIT` to `Apache-2.0`
  (the actual `LICENSE` file), and bumped `requires-python` to `>=3.11`.
- Added `classifiers` (licence + supported Python versions) to `pyproject.toml`.

## [0.1.1] - 2026-08-31

### Added

- Readability pass on Phase 4 (connected) USRN matching in `join.py`: clearer
  names (`_propagate_phase4` → `_phase4_match`, `best` → `best_match`) and
  added explanatory comments around the neighbour-seeding query and the
  unmatched-feature lookup.
- Raised the default national join chunk count from 50 to 80.

## [0.1.0] - 2026-08-30

### Added

- USRN preparation: `prepare-usrns` (Hilbert-sorted centreline GeoParquet from
  the OS Open USRN GeoPackage) and `prepare-usrns-line` (buffered corridor
  polygons for line joins).
- UPRN preparation: `prepare-uprns` (plain address points, `uprn` + `geometry`
  only) and `prepare-uprns-buffer` (buffered catchment polygons).
- Prepare pipeline for arbitrary RHS datasets: `prepare-gpkg` (GeoPackage/
  shapefile), `prepare-csv` (x/y columns or WKT text), `prepare-parquet`
  (existing GeoParquet, with optional reprojection). Every prepared file is
  Hilbert-sorted, ZSTD-compressed GeoParquet 1.1 with a `bbox` covering
  column for row-group pruning.
- Spatial joins via `match`, keyed by `(--lhs-name, --mode)`:
  - `usrn` + `polygon` — `ST_Intersects` against USRN centrelines.
  - `usrn` + `point` — nearest-USRN `ST_DWithin` match.
  - `usrn` + `line` — four-phase match (centreline intersect, corridor
    overlap, nearest fallback, connectivity inheritance).
  - `uprn` + `polygon` — `ST_Intersects` of UPRN address points against a
    polygon dataset.
- `FilteredMode` (bbox/city-scoped, single query) and `NationalMode`
  (row-group-chunked, streamed to Parquet) execution for every join.
- CLI (`geo-matcher`) and Python API (`GeoMatcher`, `DatasetConfig`,
  `prepare()`).
