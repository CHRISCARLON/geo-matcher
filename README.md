<div align='center'>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/geomatcher-mark-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="assets/geomatcher-mark-light.png">
  <img alt="GeoMatcher logo" src="assets/geomatcher-mark-light.png" width="120">
</picture>

[![CI](https://github.com/CHRISCARLON/geo-matcher/actions/workflows/ci.yml/badge.svg)](https://github.com/CHRISCARLON/geo-matcher/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/geo-matcher)](https://pypi.org/project/geo-matcher/)
[![Python](https://img.shields.io/pypi/pyversions/geo-matcher)](https://pypi.org/project/geo-matcher/)
[![Licence: Apache 2.0](https://img.shields.io/badge/licence-Apache%202.0-green.svg)](LICENSE)

</div>

# GeoMatcher

Spatially join Unique Street Reference Numbers (USRNs) & Unique Propert Reference Numbers to geospatial datasets using SedonaDB.

Built on [Apache Sedona](https://sedona.apache.org/) (Rust-based spatial query engine) for spatial joins and [DuckDB](https://duckdb.org/) for GeoParquet preparation, with optimised [GeoParquet 1.1](https://geoparquet.org/) files for use during matching.

## What it does

`geo-matcher` focuses on one thing: *spatially matching USRNs & UPRNS to other datasets*.

All geometries must be in British National Grid (EPSG:27700). 

Output is attribute-only — no geometry columns are included in the outputs as they can be joined back on later.

## Installation

```bash
git clone <repo>
cd geo-matcher
uv sync
```

## Quick start

```bash
# 1. Create project directories
geo-matcher init

# 2. Prepare USRNs (run once, or when OS Open USRN is updated)
geo-matcher prepare-usrns \
  --usrn-gpkg  input_data/osopenusrn.gpkg \
  --cache-dir  output_data

# 3. Prepare your dataset
geo-matcher prepare-gpkg \
  --rhs-name   my_dataset \
  --rhs-gpkg   input_data/my_dataset.gpkg \
  --cache-dir  output_data

# 4. Run the join
geo-matcher match \
  --rhs-name    my_dataset \   # prepared dataset name
  --mode        polygon \      # polygon, point, or line
  --city        LEEDS \        # pre-defined bbox (see bboxes.py) or use --bbox
  --output      parquet \      # parquet or csv
  --cache-dir   output_data \  # where prepared parquets live
  --matched-dir matched_data   # where output is written
```

### Line datasets (e.g. pipes, cables)

```bash
# Prepare buffered USRN corridors first
geo-matcher prepare-usrns-line \
  --buffer-m  10 \
  --cache-dir output_data

# Four-phase line join
geo-matcher match \
  --rhs-name          my_lines \                               # prepared dataset name
  --mode              line \                                   # four-phase line strategy
  --distance          10 \                                     # Phase 1+2 buffer width in metres
  --phase3-distance   15 \                                     # Phase 3 nearest-fallback radius (catches features just outside the buffer)
  --rhs-id-col        asset_id \                               # unique ID column to track matched features between phases
  --usrn-line-parquet output_data/usrns_line_10m_27700.parquet \ # buffered USRN corridors for Phase 2
  --city              LEEDS \                                  # spatial filter
  --output            csv \
  --cache-dir         output_data \
  --matched-dir       matched_data
```

### UPRN joins (address points)

```bash
# Polygon join against UPRN address points instead of USRN centrelines
geo-matcher match \
  --lhs-name    uprn \         # join from UPRN address points
  --rhs-name    my_dataset \   # prepared dataset name
  --mode        polygon \      # currently the only mode registered for --lhs-name uprn
  --city        LEEDS \
  --output      csv
```

## Docs

- [Usage — CLI & Python API](docs/usage.md)
- [Output formats & cardinality](docs/output.md)
- [How it works](docs/how-it-works.md)
- [Changelog](CHANGELOG.md)
