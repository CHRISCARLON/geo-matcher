# Benchmarks

Scripts for comparing optimised (Hilbert-sorted, bbox, covering metadata, etc) Parquet files against unoptimised exports.

## Scripts

### `export_unoptimised.py`

Exports one or more GeoPackage files to plain Parquet (ZSTD, no spatial optimisations) as a baseline.

```bash
python benchmark/export_unoptimised.py input_data/osopenusrn.gpkg input_data/soil.gpkg
# outputs: output_data/osopenusrn_unoptimised.parquet, output_data/soil_unoptimised.parquet

python benchmark/export_unoptimised.py data/roads.gpkg --out-dir results/
```

### `compare_parquet.py`

Prints row count, row group count, file size, and column list for each Parquet file.

```bash
python benchmark/compare_parquet.py output_data/usrns_unoptimised.parquet output_data/usrns_27700.parquet
```

### `bench_optimised_vs_unoptimised.py`

Runs `UsrnMatcher.match_intersect` across nine UK city bounding boxes for each supplied variant, reporting elapsed time and result row count.

```bash
python benchmark/bench_optimised_vs_unoptimised.py \
    --variant optimised   output_data/usrns_27700.parquet        output_data/soil_27700.parquet \
    --variant unoptimised output_data/usrns_unoptimised.parquet  output_data/soil_unoptimised.parquet
```

Each `--variant` takes three arguments: `label usrn_parquet soil_parquet`. Pass as many variants as you like.

## Example run

USRN dataset: 1,760,546 rows. RHS (soil): 42,603 polygons.
Variants: `usrns_27700.parquet` / `soil_27700.parquet` (optimised) vs `usrns_unoptimised.parquet` / `soil_unoptimised.parquet`.

| City        |    Rows | Optimised | Unoptimised | Speedup |
|-------------|--------:|----------:|------------:|--------:|
| London      | 112,722 |    0.27 s |      0.69 s |   2.6×  |
| Leeds       |  39,582 |    0.12 s |      0.51 s |   4.3×  |
| Manchester  |  48,931 |    0.18 s |      0.52 s |   2.9×  |
| Birmingham  |  23,359 |    0.18 s |      0.52 s |   2.9×  |
| Liverpool   |  14,316 |    0.12 s |      0.49 s |   4.1×  |
| Sheffield   |  24,022 |    0.18 s |      0.50 s |   2.8×  |
| Bristol     |  14,125 |    0.16 s |      0.51 s |   3.2×  |
| Newcastle   |  19,991 |    0.15 s |      0.50 s |   3.3×  |
| Nottingham  |  11,526 |    0.13 s |      0.50 s |   3.8×  |

Optimised files (Hilbert-sorted rows + bbox covering struct) are 2.6–4.3× faster due to better row-group pruning during the spatial filter step. 

The unoptimised baseline plateaus around 0.50 s regardless of result size because it must scan all row groups; the optimised variant scales more tightly with the number of matching rows.

Not huge gains as Parquets are fast on read io anyway but this would scale the larger the dataset.
