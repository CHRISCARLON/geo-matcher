# Usage

Three-phase pipeline:

```
prepare  →  match  →  dtf-export (optional)
```

The prepare step converts source files into spatially-sorted GeoParquet cached on disk. Run once; reuse for every subsequent match.

---

## CLI

### 1. Init

```bash
usrn-matcher init
```

Creates `input_data/`, `output_data/`, and `matched_data/`.

---

### 2. Prepare

**USRNs:**

```bash
usrn-matcher prepare-usrns
```

Reads `input_data/osopenusrn.gpkg`, writes `output_data/usrns_27700.parquet`. Add `--force` to re-prepare.

**RHS from GeoPackage / shapefile:**

```bash
usrn-matcher prepare-gpkg --rhs-gpkg input_data/dataset.gpkg --rhs-name dataset_one
```

**RHS from CSV with coordinate columns:**

```bash
usrn-matcher prepare-csv \
  --csv   input_data/stops.csv \
  --name  stops \
  --x-col Easting \
  --y-col Northing
```

**RHS from an existing Parquet (re-optimise / reproject):**

```bash
usrn-matcher prepare-parquet \
  --parquet    input_data/data.parquet \
  --name       dataset_one \
  --source-crs EPSG:4326
```

All prepare commands accept `--force` (re-prepare even if output exists) and `--threads N` (limit CPU usage).

---

### 3. Match

```bash
# Intersect join — polygon or line datasets
usrn-matcher match --rhs-name soil --city LEEDS

# Nearest join — point datasets
usrn-matcher match --rhs-name stops --city LEEDS --mode nearest --distance 25

# Full national join
usrn-matcher match --rhs-name soil --output parquet
```

| Flag | Default | Description |
|---|---|---|
| `--rhs-name` | _(required)_ | Must match name used in prepare |
| `--mode` | `intersect` | `intersect`, `nearest`, or `line` |
| `--distance` | `10` | Search radius in metres (`nearest` / `line` only) |
| `--bbox` | none | `XMIN YMIN XMAX YMAX` in EPSG:27700 |
| `--city` | none | Named bbox shortcut (e.g. `LEEDS`, `LONDON`) |
| `--geometry` | `none` | `none`, `usrn`, `clip`, or `rhs` |
| `--output` | `csv` | `csv`, `parquet`, or `sample` |
| `--batches` | `10` | Row-group batches for national joins; ignored when `--bbox`/`--city` supplied |
| `--explain` | false | Run `EXPLAIN ANALYZE` before the join |

---

### 4. DTF Export

Runs the match and writes all four DTF output formats in one step.

```bash
usrn-matcher dtf-export \
  --rhs-name     stops \
  --city         LEEDS \
  --mode         nearest \
  --distance     25 \
  --dtf-org-name "My Council" \
  --dtf-org-ref  1234
```

Writes to `matched_data/`: DTF 8.1a CSV, GeoParquet 1.1, flat CSV, and GeoPackage.
See [dtf-mapping.md](dtf-mapping.md) for the full field layout.

---

## Python API

```python
from usrn_matcher import DatasetConfig, OgrSource, CsvSource, UsrnMatcher, DTFConfig
from usrn_matcher.prepare import prepare
from usrn_matcher.dtf import to_dtf_csv, to_dtf_geoparquet, to_dtf_flat_csv, to_dtf_gpkg
from usrn_matcher.bboxes import LEEDS
import pathlib

# --- Prepare USRNs ---
prepare(DatasetConfig(
    name="usrns",
    source=OgrSource(path="input_data/osopenusrn.gpkg", row_group_size=20_000),
    parquet_path="output_data/usrns_27700.parquet",
))

# --- Prepare RHS dataset (GeoPackage) ---
prepare(DatasetConfig(
    name="stops",
    source=CsvSource(path="input_data/stops.csv", x_col="Easting", y_col="Northing"),
    parquet_path="output_data/stops_27700.parquet",
))

# --- Match ---
matcher = UsrnMatcher(
    usrn_parquet="output_data/usrns_27700.parquet",
    rhs_config=DatasetConfig(
        name="stops",
        source_path="output_data/stops_27700.parquet",
        parquet_path="output_data/stops_27700.parquet",
        columns=["ATCOCode", "CommonName"],
    ),
)

table = matcher.match_dispatch("nearest", bbox=LEEDS, distance_m=25)

# --- DTF Export ---
dtf_cfg = DTFConfig(swa_org_name="My Council", swa_org_ref=1234, rhs_name="stops")
out = pathlib.Path("matched_data")

table = matcher.match_dispatch("nearest", bbox=LEEDS, distance_m=25, include_rhs_geometry=True)
to_dtf_csv(table, dtf_cfg, out / "stops.csv")
to_dtf_geoparquet(table, dtf_cfg, out / "stops.parquet")
to_dtf_flat_csv(table, dtf_cfg, out / "stops_flat.csv")
to_dtf_gpkg(table, dtf_cfg, out / "stops.gpkg")
```

`match_dispatch` returns a `pyarrow.Table`. Pass `geometry="rhs"` to include a geometry column; pass `include_rhs_geometry=True` for the separate `rhs_geometry` column required by the DTF export functions.
