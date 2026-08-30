# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
