# Usage

Two-phase pipeline:

```
prepare  →  match
```

- Prepare converts source files into spatially-sorted GeoParquet, cached in `output_data/`.
- Run prepare once per source; reuse the output for every subsequent match.

---

## CLI

### 1. Init

```bash
geo-matcher init
```

Creates `input_data/`, `output_data/`, and `matched_data/`.

---

### 2. Prepare

**USRNs:**

```bash
geo-matcher prepare-usrns
```

- Reads `input_data/osopenusrn.gpkg`, writes `output_data/usrns_27700.parquet`.
- Add `--force` to re-prepare.

**UPRNs (address points, optional):**

```bash
geo-matcher prepare-uprns
```

- Reads `input_data/osopenuprn.gpkg`, writes `output_data/uprns_27700.parquet`.
- Keeps only `uprn` + `geometry` — the source's redundant coordinate columns are dropped.
- ~23x the row count of USRN, so it takes noticeably longer.
- Follow with `geo-matcher prepare-uprns-buffer --buffer-m 10` for buffered catchment polygons.
- That writes `uprns_buffer_10m_27700.parquet`, adding `geometry_point` for the original point.

**RHS from GeoPackage / shapefile:**

```bash
geo-matcher prepare-gpkg --rhs-name dataset_one
```

**RHS from CSV with coordinate columns:**

```bash
geo-matcher prepare-csv \
  --name  stops \
  --x-col Easting \
  --y-col Northing
```

**RHS from CSV with WKT geometry text (line/polygon):**

```bash
geo-matcher prepare-csv \
  --name          gas_pipes \
  --geometry-type line \
  --wkt-col       wkt
```

- `--wkt-col` holds plain WKT text (`LINESTRING(...)`, `MULTILINESTRING(...)`, `POLYGON(...)`, `MULTIPOLYGON(...)`).
- Required whenever `--geometry-type` is `line` or `polygon`.
- Quote the WKT column in your CSV if it contains commas — most export tools do this automatically.

**RHS from an existing Parquet (re-optimise / reproject):**

```bash
geo-matcher prepare-parquet \
  --name       dataset_one \
  --source-crs EPSG:4326
```

Every prepare command also accepts:

| Flag | Description |
|---|---|
| `--force` | Re-prepare even if the output already exists |
| `--threads N` | DuckDB thread count (default: all cores) |
| `--memory-limit SIZE` | DuckDB `memory_limit` for this run, e.g. `4GB` (default: DuckDB's own ~80% of system RAM) |

---

### 3. Match

```bash
# Polygon join — area / polygon datasets (default)
geo-matcher match --rhs-name soil --city LEEDS

# Point join — point datasets
geo-matcher match --rhs-name stops --city LEEDS --mode point --distance 25

# Line join — linestring datasets (two-phase, requires buffered USRN file)
geo-matcher match --rhs-name gas_pipe --mode line --distance 10 --rhs-id-col asset_id --city MANCHESTER

# Full national join (streaming parquet output)
geo-matcher match --rhs-name soil --output parquet

# UPRN polygon join — address points against a polygon dataset
geo-matcher match --lhs-name uprn --rhs-name soil --city LEEDS
```

| Flag | Default | Description |
|---|---|---|
| `--rhs-name` | _(required)_ | Must match name used in prepare |
| `--lhs-name` | `usrn` | Base dataset to join from: `usrn` (street centrelines) or `uprn` (address points) |
| `--mode` | `polygon` | `polygon`, `point`, or `line` (linestrings, two-phase) |
| `--distance` | `10` | Search radius in metres (`point` / `line` only) |
| `--rhs-id-col` | none | Required for `--mode line` |
| `--bbox` | none | `XMIN YMIN XMAX YMAX` in EPSG:27700 |
| `--city` | none | Named bbox shortcut (e.g. `LEEDS`, `LONDON`) |
| `--output` | `csv` | `csv`, `parquet`, or `sample` |
| `--batches` | `100` | RHS row-group chunks for national joins; ignored with `--bbox`/`--city` |
| `--rows-per-batch` | `5000` | Rows per sub-batch for `--mode line` Phases 3/4 |
| `--threads` | `4` | DataFusion target partitions |
| `--explain` | false | Run `EXPLAIN ANALYZE` before the join |

Not every `--mode` is registered for every `--lhs-name` — currently `uprn` only has `polygon`.

---

## Python API

```python
import pathlib

from geo_matcher import DatasetConfig, UsrnSource, UprnSource, CsvSource, GeoMatcher
from geo_matcher.prepare import prepare
from geo_matcher.bboxes import LEEDS

# Prepare USRNs
prepare(DatasetConfig(
    name="usrns",
    source=UsrnSource(path="input_data/osopenusrn.gpkg", row_group_size=20_000),
    parquet_path="output_data/usrns_27700.parquet",
))

# Prepare UPRNs (optional — address points, uprn + geometry only)
prepare(DatasetConfig(
    name="uprns",
    source=UprnSource(path="input_data/osopenuprn.gpkg", row_group_size=20_000),
    parquet_path="output_data/uprns_27700.parquet",
))

# Prepare RHS dataset
prepare(DatasetConfig(
    name="stops",
    source=CsvSource(path="input_data/stops.csv", x_col="Easting", y_col="Northing"),
    parquet_path="output_data/stops_27700.parquet",
))

# Match
matcher = GeoMatcher(
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

- `match_dispatch` returns a `pyarrow.Table` with attribute columns only (no geometry).
- `output_writer(table, output, matched_dir, stem)` is the single entry point for writing results.
- It picks the writer from an output-format string (`"csv"`, `"parquet"`, or `"sample"`).
- Writes to `matched_dir / f"{stem}.<ext>"` — the same dispatch the CLI `--output` flag uses.
