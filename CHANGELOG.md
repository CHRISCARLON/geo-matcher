# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `prepare(..., memory_limit=...)` now actually reaches DuckDB.
- `prepare_uprn` now follows same dispatch path as `prepare_usrn`.

### Changed

- OGR geometry is forced to 2D on write (`ST_Force2D`).
- `prepare-uprns` now shares the OGR read path.
- `prepare-uprns` validates the source CRS.
- Better handling of non 27700 CRS.

## [0.1.4] - 2026-09-15

### Fixed

- `_prepare_ogr` now stages OGR reads to a plain Parquet file before
  Hilbert-sorting, instead of driving GDAL from inside the sort query.

### Added

- `prepare(..., memory_limit=...)` — optional DuckDB memory limit, threaded
  through every `_prepare_*` function.
- `pyproj` added as an explicit dependency, used to write CRS PROJJSON into
  GeoParquet metadata.

### Changed

- `sedonadb` is no longer imported by callers that only use `prepare()` — the
  import moved into `GeoMatcher._connect()` and behind `TYPE_CHECKING`.
- `pyogrio` removed; OGR layer metadata now reads through DuckDB's own GDAL
  (`_read_ogr_info`) instead of a second bundled GDAL.

## [0.1.5] - 2026-09-20

### Changed

- Merged `join.py`'s two join dispatchers into one `execute_join`; the public
  `execute_line_join` is gone, replaced by the exported `LineJoinPhases`.
- Renamed `_national_single_phase`/`_filtered_single_phase` to
  `_national_spatial_join`/`_filtered_spatial_join` to describe intent.
- Extracted `_materialise_national`, deduplicating stream-to-output-or-tempfile
  logic shared by both `NationalMode` branches.

## [0.1.2] - 2026-08-31

### Changed

- Line-join ID bookkeeping in `join.py` moved from Python `set`s to PyArrow
  compute (`pc.unique` and new `_union_ids`/`_intersect_len` helpers).
- Added logo, CI (ruff/mypy/pytest on Python 3.11–3.13), and README badges.
- Fixed `pyproject.toml`'s license field (MIT → Apache-2.0), bumped
  `requires-python` to `>=3.11`, and added classifiers.

## [0.1.1] - 2026-08-31

### Added

- Readability pass on Phase 4 USRN matching in `join.py` (clearer names,
  added comments).
- Raised the default national join chunk count from 50 to 80.

## [0.1.0] - 2026-08-30

### Added

- USRN preparation: `prepare-usrns` and `prepare-usrns-line` (buffered
  corridors for line joins).
- UPRN preparation: `prepare-uprns` and `prepare-uprns-buffer` (buffered
  catchment polygons).
- Prepare pipeline for arbitrary RHS datasets (`prepare-gpkg`, `prepare-csv`,
  `prepare-parquet`) — all output Hilbert-sorted, ZSTD GeoParquet 1.1 with a
  `bbox` covering column.
- Spatial joins via `match`: USRN polygon/point/line joins and the UPRN
  polygon join, each with `FilteredMode` and `NationalMode` execution.
- CLI (`geo-matcher`) and Python API (`GeoMatcher`, `DatasetConfig`,
  `prepare()`).
