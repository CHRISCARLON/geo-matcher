"""Summarise row counts, row groups, size and schema for Parquet files.

Some basic comparisons between the optimised vs non-optimised.

Usage:
    python compare_parquet.py output_data/usrns_unoptimised.parquet output_data/usrns_27700.parquet
"""

import argparse
import pathlib

import pyarrow.parquet as pq

parser = argparse.ArgumentParser(description="Compare Parquet file metadata.")
parser.add_argument(
    "files", nargs="+", type=pathlib.Path, help="Parquet file(s) to inspect"
)
args = parser.parse_args()

for path in args.files:
    meta = pq.read_metadata(str(path))
    size_mb = path.stat().st_size / 1024 / 1024
    print(f"{path}:")
    print(
        f"  rows={meta.num_rows:,}  row_groups={meta.num_row_groups}  size={size_mb:.1f}MB"
    )
    schema = pq.read_schema(str(path))
    print(f"  columns={schema.names}")
    print()
