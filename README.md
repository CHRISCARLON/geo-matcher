# USRN Matcher

Spatially join Unique Street Reference Numbers (USRNs) to any geospatial dataset using SedonaDB.

Built on [Apache Sedona](https://sedona.apache.org/) (Rust-based spatial query engine) for spatial joins and [DuckDB](https://duckdb.org/) for GeoParquet preparation, with optimised [GeoParquet 1.1](https://geoparquet.org/) output.

## What it does

`usrn-matcher` answers the question: *which USRN does this spatial feature interact with?*

Given any spatial dataset (bus stops, soil polygons, flood zones — anything with a geometry), it finds the USRN or USRNs that intersect or are nearest to each feature and produces a joined output carrying both the USRN reference and the original dataset's attributes.

It expects all geometries to already be in British National Grid/27700.

There are two output routes:

| Route | Command | Geometry kept | Best for |
|---|---|---|---|
| **Standard** | `usrn-matcher match` | Optional — controlled by `--geometry` flag | Street-centric analysis |
| **DTF export** | `usrn-matcher dtf-export` | Matched RHS feature geometry | Dataset-centric exchange in DTF8.1-inspired format |

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

# 2. Prepare source files (run once)
usrn-matcher prepare-usrns
usrn-matcher prepare-gpkg \
  --rhs-gpkg input_data/dataset.gpkg \
  --rhs-name dataset_one

# 3a. Run spatial join
usrn-matcher match --rhs-name dataset_one --city LEEDS

# 3b. Or export in DTF8.1-inspired format (optional)
usrn-matcher dtf-export \
  --rhs-name     dataset_one \
  --city         LEEDS \
  --dtf-org-name "My Org" \
  --dtf-org-ref  1234
```

## Docs

- [Usage — CLI & Python API](docs/usage.md)
- [Output formats & cardinality](docs/output.md)
- [How it works](docs/how-it-works.md)
- [DTF8.1 mapping](docs/dtf-mapping.md)
