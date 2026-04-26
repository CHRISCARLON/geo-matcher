"""Benchmark Parquet variants across multiple city bboxes.

Each --variant takes three arguments: label usrn_parquet soil_parquet

Usage:
    python bench_optimised_vs_unoptimised.py \\
        --variant optimised output_data/usrns_27700.parquet output_data/soil_27700.parquet \\
        --variant unoptimised output_data/usrns_unoptimised.parquet output_data/soil_unoptimised.parquet
"""

import argparse
import time

from usrn_matcher import DatasetConfig, UsrnMatcher
from usrn_matcher.bboxes import (
    BIRMINGHAM,
    BRISTOL,
    LEEDS,
    LIVERPOOL,
    LONDON,
    MANCHESTER,
    NEWCASTLE,
    NOTTINGHAM,
    SHEFFIELD,
)

CITIES: dict[str, list[int]] = {
    "London": LONDON,
    "Leeds": LEEDS,
    "Manchester": MANCHESTER,
    "Birmingham": BIRMINGHAM,
    "Liverpool": LIVERPOOL,
    "Sheffield": SHEFFIELD,
    "Bristol": BRISTOL,
    "Newcastle": NEWCASTLE,
    "Nottingham": NOTTINGHAM,
}

parser = argparse.ArgumentParser(description="Benchmark Parquet variants.")
parser.add_argument(
    "--variant",
    nargs=3,
    metavar=("LABEL", "USRN_PARQUET", "SOIL_PARQUET"),
    action="append",
)
args = parser.parse_args()

_DEFAULT_VARIANTS = [
    ("optimised",   "output_data/usrns_27700.parquet",        "output_data/soil_27700.parquet"),
    ("unoptimised", "output_data/usrns_unoptimised.parquet",  "output_data/soil_unoptimised.parquet"),
]

variants = [
    {"label": label, "usrn_parquet": usrn, "soil_parquet": soil}
    for label, usrn, soil in (args.variant or _DEFAULT_VARIANTS)
]


def _run(label: str, usrn_parquet: str, soil_parquet: str, bbox: list[int]) -> tuple[float, int]:
    soil_cfg = DatasetConfig(
        name="soil",
        source_path="input_data/soil.gpkg",
        parquet_path=soil_parquet,
    )
    matcher = UsrnMatcher(usrn_parquet=usrn_parquet, rhs_config=soil_cfg)
    t0 = time.perf_counter()
    result = matcher.match_intersect(bbox=bbox)
    return time.perf_counter() - t0, len(result)


for city, bbox in CITIES.items():
    print(f"\n=== {city} ===")
    for v in variants:
        elapsed, rows = _run(v["label"], v["usrn_parquet"], v["soil_parquet"], bbox)
        print(f"  {v['label']:14s}  {elapsed:.2f}s  {rows:,} rows")
