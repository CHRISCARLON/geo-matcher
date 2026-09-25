# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-09-25

### Fixed

- `prepare(..., memory_limit=...)` now actually reaches DuckDB.
- `prepare_uprn` now follows same dispatch path as `prepare_usrn`.
- `_prepare_parquet` re-preparing the pipeline's own output no longer fails
  with a DuckDB binder error from `ST_GeomFromWKB` rejecting `GEOMETRY`.

### Added

- `nadgrids_path` on every `*Source` — points at an OS OSTN15 NTv2 `.gsb`
  grid for accurate EPSG:27700 transforms; without it a warning now fires.
- DuckDB's compiled PROJ version is logged at the start of every prepare run.
- `_prepare_parquet` now detects external sources' CRS from GeoParquet `geo`
  metadata, raising if it can't, instead of assuming it matches `target_crs`.
- `OgrSource`/`CsvSource`/`ParquetSource`: `crs` renamed to `target_crs`,
  plus a new optional `source_crs` — what you declare the file is in.
- Prepare auto-detects each source's CRS (file metadata for OGR/Parquet,
  coordinate extent for CSV) and warns if a declared `source_crs` disagrees.
- `prepare-gpkg` gained `--crs`/`--source-crs` CLI flags (previously only on
  `prepare-csv`/`prepare-parquet`), so GPKG sources can use both from the CLI.

### Changed

- OGR geometry is forced to 2D on write (`ST_Force2D`).
- `prepare-uprns` now shares the OGR read path.
- `prepare-uprns` validates the source CRS.
- Better handling of non 27700 CRS.
- All `_prepare_*` dispatchers now share one `_prepare_common` skeleton,
  replacing duplicated per-function logging that had drifted out of sync.
- `_prepare_ogr`/`_prepare_csv`/`_prepare_parquet` now share one
  `_crs_log_desc` helper for their CRS log line and OSTN15 decision.
- `_prepare_parquet` now always detects/transforms CRS and raises if it
  can't.
- Kept only critical tests and deleted redundant ones.
- Merged `join.py`'s two join dispatchers into one `execute_join`; the public
  `execute_line_join` is gone, replaced by the exported `LineJoinPhases`.
- Renamed `_national_single_phase`/`_filtered_single_phase` to
  `_national_spatial_join`/`_filtered_spatial_join` to describe intent.
- Extracted `_materialise_national`, deduplicating stream-to-output-or-tempfile
  logic shared by both `NationalMode` branches.

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
- Prepare pipeline for arbitrary RHS datasets (`prepare-gpkg/-csv/-parquet`)
  — Hilbert-sorted, ZSTD GeoParquet 1.1 with a `bbox` covering column.
- Spatial joins via `match`: USRN polygon/point/line joins and the UPRN
  polygon join, each with `FilteredMode` and `NationalMode` execution.
- CLI (`geo-matcher`) and Python API (`GeoMatcher`, `DatasetConfig`,
  `prepare()`).
