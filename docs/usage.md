# Usage

There are two ways to use usrn-matcher: the **CLI** and the **Python API**.
Both follow the same three-phase pipeline:

```
prepare  →  match  →  export
```

The prepare step is slow and only needs to run once — it converts source files into
spatially-sorted GeoParquet that is cached on disk and reused for every subsequent match.

---

## CLI

The CLI is the simplest entry point. All commands go through `UsrnMatcher.cli()`.

### 1. Initialise project directories

```bash
usrn-matcher init
```

Creates `input_data/`, `output_data/`, and `matched_data/` in the current directory.

---

### 2. Prepare (pre-spatial phase)

Convert source files into spatially-sorted GeoParquet. Run once, or with `--force` to
re-prepare.

**From a GeoPackage or shapefile:**

```bash
usrn-matcher prepare \
  --usrn-gpkg input_data/osopenusrn.gpkg \
  --rhs-gpkg  input_data/dataset.gpkg \
  --rhs-name  dataset_one
```

Writes:
- `output_data/usrns_27700.parquet`
- `output_data/dataset_one_27700.parquet`

Key options:

| Flag | Default | Description |
|---|---|---|
| `--usrn-gpkg` | `input_data/osopenusrn.gpkg` | OS Open USRN source |
| `--rhs-gpkg` | _(required)_ | RHS source file |
| `--rhs-name` | _(required)_ | Short identifier, used as filename stem |
| `--rhs-geometry-col` | `geometry` | Geometry column name in RHS file |
| `--cache-dir` | `output_data` | Where to write GeoParquet files |
| `--force` | false | Re-prepare even if output already exists |

**From a CSV with coordinate columns:**

```bash
usrn-matcher prepare-csv \
  --csv   input_data/stops.csv \
  --name  stops \
  --x-col Easting \
  --y-col Northing
```

Key options:

| Flag | Default | Description |
|---|---|---|
| `--csv` | _(required)_ | Source CSV path |
| `--name` | _(required)_ | Short identifier |
| `--x-col` | `Easting` | X / Easting column |
| `--y-col` | `Northing` | Y / Northing column |
| `--crs` | `EPSG:27700` | CRS of the coordinate columns |
| `--cache-dir` | `output_data` | Where to write GeoParquet files |

---

### 3. Match (spatial phase)

```bash
# Intersect join — for polygon or line datasets
usrn-matcher match --rhs-name stops --city LEEDS

# Nearest join — for point datasets
usrn-matcher match --rhs-name stops --city LEEDS --mode nearest --distance 25
```

Key options:

| Flag | Default | Description |
|---|---|---|
| `--rhs-name` | _(required)_ | Must match the name used in prepare |
| `--mode` | `intersect` | `intersect` or `nearest` |
| `--distance` | `50` | Search radius in metres (nearest only) |
| `--bbox` | none | `XMIN YMIN XMAX YMAX` in EPSG:27700 |
| `--city` | none | Named bbox shortcut (see `bboxes.py`) |
| `--rhs-columns` | all | Columns to select from RHS dataset |
| `--output` | `csv` | `csv`, `parquet`, or `sample` |
| `--cache-dir` | `output_data` | Where to find prepared GeoParquet |
| `--matched-dir` | `matched_data` | Where to write output |
| `--explain` | false | Run `EXPLAIN ANALYZE` before join |

Omit `--bbox` / `--city` for a full national join (slow).

---

### 4. Export (DTF8.1)

Runs the match and writes all four DTF output formats in one step.

```bash
usrn-matcher export \
  --rhs-name     stops \
  --city         LEEDS \
  --mode         nearest \
  --distance     25 \
  --dtf-org-name "My Council" \
  --dtf-org-ref  1234
```

Writes to `matched_data/`:
- `usrn_stops_attribution.csv` — DTF8.1a CSV (type 10/69/63a/67a/99 records)
- `usrn_stops_attribution.parquet` — GeoParquet 1.1
- `usrn_stops_attribution_flat.csv` — flat CSV
- `usrn_stops_attribution.gpkg` — GeoPackage

Key options (in addition to match options):

| Flag | Default | Description |
|---|---|---|
| `--dtf-org-name` | `usrn-matcher` | Organisation name in DTF type 10 header |
| `--dtf-org-ref` | `0` | SWA organisation reference code |

---

## Python API

For scripting or integration into a larger pipeline, import directly.
Prepare functions and export functions are standalone — `UsrnMatcher` owns only the
match step.

### 1. Prepare

```python
from usrn_matcher import DatasetConfig, prepare_dataset, prepare_usrns
from usrn_matcher import prepare_from_csv  # CSV variant

# Prepare USRNs
prepare_usrns(
    usrn_gpkg="input_data/osopenusrn.gpkg",
    parquet_path="output_data/usrns_27700.parquet",
)

# Prepare RHS dataset (GeoPackage / shapefile)
cfg = DatasetConfig(
    name="stops",
    source_path="input_data/naptan_stops.gpkg",
    parquet_path="output_data/stops_27700.parquet",
    columns=["ATCOCode", "CommonName", "StopType"],
    row_group_size=10_000,
)
prepare_dataset(cfg)

# Or from CSV
prepare_from_csv(
    csv_path="input_data/stops.csv",
    parquet_path="output_data/stops_27700.parquet",
    x_col="Easting",
    y_col="Northing",
)
```

Pass `force=True` to either function to re-prepare even if the output already exists.

---

### 2. Match

> **Prepare must be run first.** `UsrnMatcher` expects both `usrns_27700.parquet` and
> `{name}_27700.parquet` to already exist in `output_data/`. Run the prepare steps above
> before constructing a matcher.

```python
from usrn_matcher import DatasetConfig, UsrnMatcher
from usrn_matcher.bboxes import LEEDS

cfg = DatasetConfig(
    name="stops",
    source_path="output_data/stops_27700.parquet",
    parquet_path="output_data/stops_27700.parquet",
    columns=["ATCOCode", "CommonName", "StopType"],
)

matcher = UsrnMatcher(
    usrn_parquet="output_data/usrns_27700.parquet",
    rhs_config=cfg,
)

# Intersect join — polygons / lines
table = matcher.match_intersect(bbox=LEEDS)

# Nearest join — points
table = matcher.match_nearest(bbox=LEEDS, distance_m=25)
```

Both methods return a `pyarrow.Table` with columns: `usrn`, `street_type`, `geometry`,
plus all selected RHS columns.

Pass `include_rhs_geometry=True` if you intend to export to DTF format.

---

### 3. Export

The export functions operate directly on the `pyarrow.Table` returned from a match.
`UsrnMatcher` does not wrap them.

```python
import pathlib
from usrn_matcher import DTFConfig
from usrn_matcher.dtf import (
    to_dtf_csv,
    to_dtf_geoparquet,
    to_dtf_flat_csv,
    to_dtf_gpkg,
)

dtf_cfg = DTFConfig(
    swa_org_name="My Council",
    swa_org_ref=1234,
    rhs_name="stops",
)

out = pathlib.Path("matched_data")

# match must have been run with include_rhs_geometry=True
table = matcher.match_nearest(bbox=LEEDS, distance_m=25, include_rhs_geometry=True)

to_dtf_csv(table, dtf_cfg, out / "stops.csv")
to_dtf_geoparquet(table, dtf_cfg, out / "stops.parquet")
to_dtf_flat_csv(table, dtf_cfg, out / "stops_flat.csv")
to_dtf_gpkg(table, dtf_cfg, out / "stops.gpkg")
```

---

## Which to use?

| | CLI | Python API |
|---|---|---|
| One-off / exploratory runs | ✓ | |
| Scripted / automated pipelines | | ✓ |
| Integrate into larger codebase | | ✓ |
| Custom prepare logic | | ✓ |
| Quickest path to output files | ✓ | |
