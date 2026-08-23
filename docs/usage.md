# Usage

Two-phase pipeline:

```
prepare  →  match
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
usrn-matcher prepare-gpkg --rhs-name dataset_one
```

**RHS from CSV with coordinate columns:**

```bash
usrn-matcher prepare-csv \
  --name  stops \
  --x-col Easting \
  --y-col Northing
```

**RHS from CSV with WKT geometry text (line/polygon):**

```bash
usrn-matcher prepare-csv \
  --name          gas_pipes \
  --geometry-type line \
  --wkt-col       wkt
```

`--wkt-col` holds plain WKT text (`LINESTRING(...)`, `MULTILINESTRING(...)`,
`POLYGON(...)` or `MULTIPOLYGON(...)`) and is required whenever `--geometry-type` is
`line` or `polygon`. If a WKT value contains commas (most do), make sure your CSV
quotes that field — most spreadsheet/export tools do this automatically.

**RHS from an existing Parquet (re-optimise / reproject):**

```bash
usrn-matcher prepare-parquet \
  --name       dataset_one \
  --source-crs EPSG:4326
```

All prepare commands accept `--force` (re-prepare even if output exists) and `--threads N` (limit CPU usage).

---

### 3. Match

```bash
# Polygon join — area / polygon datasets (default)
usrn-matcher match --rhs-name soil --city LEEDS

# Point join — point datasets
usrn-matcher match --rhs-name stops --city LEEDS --mode point --distance 25

# Line join — linestring datasets (two-phase, requires buffered USRN file)
usrn-matcher match --rhs-name gas_pipe --mode line --distance 10 --rhs-id-col asset_id --city MANCHESTER

# Full national join (streaming parquet output)
usrn-matcher match --rhs-name soil --output parquet
```

| Flag | Default | Description |
|---|---|---|
| `--rhs-name` | _(required)_ | Must match name used in prepare |
| `--mode` | `polygon` | `polygon` (area datasets), `point` (point datasets), or `line` (linestring datasets — two-phase) |
| `--distance` | `10` | Search radius in metres (`point` / `line` only) |
| `--rhs-id-col` | none | Required for `--mode line` |
| `--bbox` | none | `XMIN YMIN XMAX YMAX` in EPSG:27700 |
| `--city` | none | Named bbox shortcut (e.g. `LEEDS`, `LONDON`) |
| `--output` | `csv` | `csv`, `parquet`, or `sample` |
| `--batches` | `50` | RHS row-group chunks for national joins (>= 2); ignored when `--bbox`/`--city` supplied |
| `--threads` | 4 | DataFusion target partitions |
| `--explain` | false | Run `EXPLAIN ANALYZE` before the join |

---

## Python API

```python
import pathlib

from usrn_matcher import DatasetConfig, UsrnSource, CsvSource, UsrnMatcher
from usrn_matcher.prepare import prepare
from usrn_matcher.bboxes import LEEDS

# Prepare USRNs
prepare(DatasetConfig(
    name="usrns",
    source=UsrnSource(path="input_data/osopenusrn.gpkg", row_group_size=20_000),
    parquet_path="output_data/usrns_27700.parquet",
))

# Prepare RHS dataset
prepare(DatasetConfig(
    name="stops",
    source=CsvSource(path="input_data/stops.csv", x_col="Easting", y_col="Northing"),
    parquet_path="output_data/stops_27700.parquet",
))

# Match
matcher = UsrnMatcher(
    usrn_parquet="output_data/usrns_27700.parquet",
    rhs_config=DatasetConfig(
        name="stops",
        source_path="output_data/stops_27700.parquet",
        parquet_path="output_data/stops_27700.parquet",
        columns=["ATCOCode", "CommonName"],
    ),
)

table = matcher.match_dispatch("point", bbox=LEEDS, distance_m=25)
matcher.output_writer(
    table,
    output="csv",
    matched_dir=pathlib.Path("matched_data"),
    stem="usrn_stops_attribution",
)
```

`match_dispatch` returns a `pyarrow.Table` with attribute columns only (no geometry).

`output_writer(table, output, matched_dir, stem)` is the single entry point for writing
results: it picks the writer from an output-format string (`"csv"`, `"parquet"`, or
`"sample"`) and writes to `matched_dir / f"{stem}.<ext>"` — the same dispatch the CLI
`--output` flag uses.
