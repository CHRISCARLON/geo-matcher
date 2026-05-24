# Benchmarks

Scripts for comparing optimised (Hilbert-sorted, GeoParquet 1.1 bbox covering) vs unoptimised Parquet exports across DuckDB and SedonaDB.

The benchmark measures a single operation: **reading USRNs from a Parquet file filtered to a city bounding box**. Nothing else — no joins, no matching. The goal is to show how the optimised file format reduces the amount of data each engine has to touch.

## Scripts

### `export_unoptimised.py`

Exports GeoPackage or CSV files to plain Parquet (ZSTD, no spatial optimisations) as a baseline. Add `--shuffle` to randomise row order.

```bash
python benchmark/export_unoptimised.py input_data/osopenusrn.gpkg
python benchmark/export_unoptimised.py input_data/stops.csv
python benchmark/export_unoptimised.py input_data/osopenusrn.gpkg --shuffle
```

### `compare_parquet.py`

Prints row count, row group count, file size, and column list for each Parquet file.

```bash
python benchmark/compare_parquet.py output_data/usrns_27700.parquet output_data/osopenusrn_unoptimised.parquet
```

### `bench_optimised_vs_unoptimised.py`

Reads USRN rows within a city bbox from each variant, reporting elapsed time, result row count, rows physically scanned from Parquet, and Sedona row-group pruning stats.

```bash
python benchmark/bench_optimised_vs_unoptimised.py
python benchmark/bench_optimised_vs_unoptimised.py \
    --variant optimised   output_data/usrns_27700.parquet \
    --variant unoptimised output_data/osopenusrn_unoptimised.parquet
```

DuckDB uses the `bbox` struct predicate on optimised files (column statistics prune row groups). Sedona uses `ST_Intersects` and reads the GeoParquet 1.1 covering metadata to prune row groups spatially.

**Note:** the bbox struct predicate is approximate — it tests bounding-box overlap, not exact geometry intersection, so it can return a small number of false positives (geometries whose bbox overlaps the query area but whose actual linestring does not). The small row-count differences between DuckDB bbox struct and ST_Intersects in the results below are examples of this.

## Results (USRN, 1.76M rows, 2026-05-10)

### London bbox

```
[optimised]  row_groups=89
  DuckDB   (bbox struct )  0.131s  out=93,328 / scanned=395,628 / file=1,760,546 rows
  DuckDB   (ST_Intersects)  0.908s  out=93,321 / scanned=1,760,546 / file=1,760,546 rows  ← no pruning
  Sedona   (ST_Intersects)  0.153s  out=93,321 / scanned=400,000 / file=1,760,546 rows
    pruning: row_groups_pruned_statistics=89/89 rgs  row_groups_spatial_pruned=20/89 rgs

[unoptimised]  row_groups=15
  DuckDB   (ST_Intersects)  0.853s  out=93,321 / scanned=1,760,546 / file=1,760,546 rows
  Sedona   (ST_Intersects)  0.315s  out=93,321 / scanned=1,760,546 / file=1,760,546 rows
    pruning: row_groups_pruned_statistics=15/15 rgs  row_groups_spatial_pruned=0/0 rgs
```

### Liverpool bbox

```
[optimised]  row_groups=89
  DuckDB   (bbox struct )  0.024s  out=12,676 / scanned=158,251 / file=1,760,546 rows
  DuckDB   (ST_Intersects)  0.795s  out=12,668 / scanned=1,760,546 / file=1,760,546 rows  ← no pruning
  Sedona   (ST_Intersects)  0.068s  out=12,668 / scanned=160,000 / file=1,760,546 rows
    pruning: row_groups_pruned_statistics=89/89 rgs  row_groups_spatial_pruned=8/89 rgs

[unoptimised]  row_groups=15
  DuckDB   (ST_Intersects)  0.818s  out=12,668 / scanned=1,760,546 / file=1,760,546 rows
  Sedona   (ST_Intersects)  0.322s  out=12,668 / scanned=1,760,546 / file=1,760,546 rows
    pruning: row_groups_pruned_statistics=15/15 rgs  row_groups_spatial_pruned=0/0 rgs
```

### What the numbers show

The optimised file prunes most row groups for each city (London: 20/89 matched, Liverpool: 8/89 matched), so both engines only scan a small fraction of the file.

| Engine | File | Strategy | London | Liverpool |
|--------|------|----------|--------|-----------|
| DuckDB | optimised | bbox struct (column stats) | 0.13s / 396K scanned | 0.02s / 158K scanned |
| Sedona | optimised | ST_Intersects + covering | 0.15s / 400K scanned | 0.07s / 160K scanned |
| DuckDB | optimised | ST_Intersects (no pruning) | 0.91s / 1.76M scanned | 0.80s / 1.76M scanned |
| DuckDB | unoptimised | ST_Intersects (no pruning) | 0.85s / 1.76M scanned | 0.82s / 1.76M scanned |
| Sedona | unoptimised | ST_Intersects (no pruning) | 0.32s / 1.76M scanned | 0.32s / 1.76M scanned |

**DuckDB bbox struct** prunes row groups via parquet column statistics on the `bbox` struct sub-fields — fast because it avoids geometry decoding entirely for skipped row groups.

**Sedona ST_Intersects on the optimised file** prunes via the GeoParquet 1.1 `covering` metadata embedded at write time — same row groups matched, slightly slower due to Spark overhead but returns exact results.

**Unoptimised files** have no spatial ordering so all row groups are scanned every time, regardless of engine or strategy.
