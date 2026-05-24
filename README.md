# USRN Matcher

Spatially join Unique Street Reference Numbers (USRNs) to any geospatial dataset using SedonaDB.

Built on [Apache Sedona](https://sedona.apache.org/) (Rust-based spatial query engine) for spatial joins and [DuckDB](https://duckdb.org/) for GeoParquet preparation, with optimised [GeoParquet 1.1](https://geoparquet.org/) output.

## What it does

`usrn-matcher` answers the question: *which USRN does this spatial feature interact with?*

Given any spatial dataset (bus stops, soil polygons, flood zones — anything with a geometry), it finds the USRN or USRNs that intersect or are nearest to each feature and produces a joined output carrying both the USRN reference and the original dataset's attributes.

All geometries must be in British National Grid (EPSG:27700). Output is attribute-only — no geometry column.

## Installation

```bash
git clone <repo>
cd usrn-matcher
uv sync
```

## Quick start

```bash
# 1. Create project directories
usrn-matcher init

# 2. Prepare USRNs (run once, or when OS Open USRN is updated)
usrn-matcher prepare-usrns \
  --usrn-gpkg  input_data/osopenusrn.gpkg \
  --cache-dir  output_data

# 3. Prepare your dataset
usrn-matcher prepare-gpkg \
  --rhs-name   my_dataset \
  --rhs-gpkg   input_data/my_dataset.gpkg \
  --cache-dir  output_data

# 4. Run the join
usrn-matcher match \
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
usrn-matcher prepare-usrns-line \
  --buffer-m  10 \
  --cache-dir output_data

# Three-phase line join
usrn-matcher match \
  --rhs-name          my_lines \                               # prepared dataset name
  --mode              line \                                   # three-phase line strategy
  --distance          10 \                                     # Phase 1+2 buffer width in metres
  --phase3-distance   15 \                                     # Phase 3 nearest-fallback radius (catches features just outside the buffer)
  --rhs-id-col        asset_id \                               # unique ID column to track matched features between phases
  --usrn-line-parquet output_data/usrns_line_10m_27700.parquet \ # buffered USRN corridors for Phase 2
  --city              LEEDS \                                  # spatial filter
  --output            csv \
  --cache-dir         output_data \
  --matched-dir       matched_data
```

## Docs

- [Usage — CLI & Python API](docs/usage.md)
- [Output formats & cardinality](docs/output.md)
- [How it works](docs/how-it-works.md)
