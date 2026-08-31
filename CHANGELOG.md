# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
