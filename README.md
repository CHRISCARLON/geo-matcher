<div align='center'>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/geomatcher-mark-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="assets/geomatcher-mark-light.png">
  <img alt="GeoMatcher logo" src="assets/geomatcher-mark-light.png" width="120">
</picture>

[![CI](https://github.com/CHRISCARLON/geo-matcher/actions/workflows/ci.yml/badge.svg)](https://github.com/CHRISCARLON/geo-matcher/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-0.1.6-blue.svg)](CHANGELOG.md)
[![Python](https://img.shields.io/badge/python-%3E%3D3.11-blue.svg)](pyproject.toml)
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

## Quick CLI Start Guide

Each block shows every flag the command accepts, with its default. 
See [`docs/usage.md`](docs/usage.md) for the same reference in table form.

```bash
# 1. Create project directories
geo-matcher init
```

```bash
# 2. Prepare USRNs (run once, or when OS Open USRN is updated)
geo-matcher prepare-usrns \
  --usrn-gpkg    input_data/osopenusrn.gpkg \  # OS Open USRN GeoPackage (default: input_data/osopenusrn.gpkg)
  --cache-dir    output_data \                 # where prepared parquets are written (default: output_data)
  --force \                                    # re-prepare even if output already exists
  --threads      4 \                           # DuckDB thread count (default: all cores)
  --memory-limit 4GB                           # DuckDB memory_limit (default: DuckDB's own ~80% of system RAM)
```

```bash
# 3. Prepare your dataset to match
geo-matcher prepare-gpkg \
  --rhs-name           my_dataset \                # short identifier, valid SQL identifier (required)
  --rhs-gpkg           input_data/my_dataset.gpkg \ # source file (default: input_data/{rhs-name}.gpkg)
  --rhs-geometry-col   geometry \                   # geometry column in the source file (default: geometry)
  --rhs-row-group-size 10000 \                      # output row group size (default: 10000) — see docs/performance.md
  --crs                EPSG:27700 \                 # CRS to reproject into (default: EPSG:27700)
  --source-crs         EPSG:4326 \                  # CRS you believe the source is in (optional — the file's own CRS always wins)
  --cache-dir          output_data \                # where prepared parquets are written (default: output_data)
  --force \                                         # re-prepare even if output already exists
  --threads            4 \                          # DuckDB thread count (default: all cores)
  --memory-limit       4GB                          # DuckDB memory_limit (default: DuckDB's own ~80% of system RAM)
```

```bash
# 4. Run the join
geo-matcher match \
  --lhs-name    usrn \          # usrn — street centrelines (default), or uprn — address points
  --rhs-name    my_dataset \    # prepared dataset name (required)
  --rhs-columns col_a col_b \   # RHS columns to keep (default: all except geometry/bbox)
  --mode        polygon \       # polygon (default), point, or line
  --city        LEEDS \         # pre-defined bbox (see bboxes.py) — or use --bbox XMIN YMIN XMAX YMAX; omit both for a national join
  --output      parquet \       # csv (default), parquet, or sample
  --sample-rows 100000 \        # row count for --output sample (default: 100000)
  --explain \                   # run EXPLAIN ANALYZE before the join (doubles run time)
  --threads     4 \             # DataFusion target partitions (default: 4)
  --cache-dir   output_data \   # where prepared parquets live (default: output_data)
  --matched-dir matched_data    # where output is written (default: matched_data)
```

### Line datasets (e.g. pipes, cables, roads, etc)

```bash
# Prepare buffered USRN corridors first
geo-matcher prepare-usrns-line \
  --buffer-m        10 \                             # corridor buffer radius in metres (required)
  --usrn-parquet    output_data/usrns_27700.parquet \ # source centrelines (default: {cache-dir}/usrns_27700.parquet)
  --cache-dir       output_data \                     # where prepared parquets are written (default: output_data)
  --row-group-size  20000 \                           # output row group size (default: 20000)
  --force \                                           # re-prepare even if output already exists
  --threads         4 \                               # DuckDB thread count (default: all cores)
  --memory-limit    4GB                               # DuckDB memory_limit (default: DuckDB's own ~80% of system RAM)

# Four-phase line join
geo-matcher match \
  --lhs-name          usrn \                                     # line join is usrn-only
  --rhs-name          my_lines \                                 # prepared dataset name (required)
  --rhs-columns       asset_id material \                        # RHS columns to keep (default: all)
  --mode              line \                                     # four-phase corridor match
  --distance          10 \                                       # Phase 1+2 corridor width in metres (default: 10)
  --phase3-distance   15 \                                       # Phase 3 nearest-fallback radius (default: same as --distance)
  --rhs-id-col        asset_id \                                 # unique ID column tracking matches between phases (required)
  --usrn-line-parquet output_data/usrns_line_10m_27700.parquet \ # buffered corridors for Phase 2, from prepare-usrns-line (required)
  --overlap-threshold 0.10 \                                     # min Phase 2 corridor overlap fraction (default: 0.10)
  --phase4-tolerance  5 \                                        # Phase 4 connectivity tolerance in metres (default: 5; 0 disables Phase 4)
  --rows-per-batch    5000 \                                     # Phase 3/4 sub-batch size (default: 5000) — see docs/performance.md
  --city              LEEDS \                                    # spatial filter — or use --bbox XMIN YMIN XMAX YMAX; omit both for national
  --output            csv \                                      # csv (default), parquet, or sample
  --sample-rows       100000 \                                   # row count for --output sample (default: 100000)
  --explain \                                                    # run EXPLAIN ANALYZE before the join
  --threads           4 \                                        # DataFusion target partitions (default: 4)
  --cache-dir         output_data \                              # where prepared parquets live (default: output_data)
  --matched-dir       matched_data                                # where output is written (default: matched_data)
```

### National joins & performance tuning

```bash
# Full national join (no bbox) — RHS is chunked so only one chunk is in
# memory at a time, with the next chunk's read overlapped with the
# current chunk's query
geo-matcher match \
  --lhs-name    usrn \          # usrn (default) or uprn
  --rhs-name    my_dataset \    # prepared dataset name (required)
  --rhs-columns col_a col_b \   # RHS columns to keep (default: all)
  --mode        polygon \       # polygon (default), point, or line — no --bbox/--city, so this runs nationally
  --batches     100 \           # RHS row-group chunks (default: 100) — ignored if --bbox/--city is given
  --threads     4 \             # DataFusion target partitions (default: 4)
  --output      parquet \       # csv (default), parquet, or sample
  --sample-rows 100000 \        # row count for --output sample (default: 100000)
  --explain \                   # run EXPLAIN ANALYZE before the join
  --cache-dir   output_data \   # where prepared parquets live (default: output_data)
  --matched-dir matched_data    # where output is written (default: matched_data)
```

Every `prepare-*` command also accepts `--threads`/`--memory-limit` (shown in
step 2/3 above), and `match` accepts `--rows-per-batch` for `--mode line`
national joins (shown in the line-datasets example above). See
[`docs/performance.md`](docs/performance.md) for how `--batches` (row-group
chunks), `--rows-per-batch`, and prepare's `--row-group-size` relate to each
other, and how the read-ahead works.

### UPRN joins (address points)

```bash
# Polygon join against UPRN address points instead of USRN centrelines
geo-matcher match \
  --lhs-name    uprn \          # join from UPRN address points instead of USRN centrelines
  --rhs-name    my_dataset \    # prepared dataset name (required)
  --rhs-columns col_a col_b \   # RHS columns to keep (default: all)
  --mode        polygon \       # currently the only mode registered for --lhs-name uprn
  --city        LEEDS \         # spatial filter — or use --bbox XMIN YMIN XMAX YMAX; omit both for a national join
  --output      csv \           # csv (default), parquet, or sample
  --sample-rows 100000 \        # row count for --output sample (default: 100000)
  --explain \                   # run EXPLAIN ANALYZE before the join
  --threads     4 \             # DataFusion target partitions (default: 4)
  --cache-dir   output_data \   # where prepared parquets live (default: output_data)
  --matched-dir matched_data    # where output is written (default: matched_data)
```

## Docs

- [Usage — CLI & Python API](docs/usage.md)
- [Output formats & cardinality](docs/output.md)
- [How it works](docs/how-it-works.md)
- [Performance internals — row groups, chunks, batches, prefetch](docs/performance.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
