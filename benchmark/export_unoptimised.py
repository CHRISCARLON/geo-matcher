"""Export plain (unoptimised) Parquet files

Produces one <stem>_unoptimised.parquet per input file.

No Hilbert sort, no bbox covering struct, no metadata patching — just a raw
DuckDB COPY TO so the optimised files have something concrete to compare against.

Usage:
    python export_unoptimised.py input_data/osopenusrn.gpkg input_data/soil.gpkg
    python export_unoptimised.py data/roads.gpkg --out-dir results/
"""

import argparse
import pathlib

import duckdb

parser = argparse.ArgumentParser(
    description="Export unoptimised Parquet from GeoPackage files."
)
parser.add_argument("sources", nargs="+", type=pathlib.Path, help="Input .gpkg file(s)")
parser.add_argument("--out-dir", type=pathlib.Path, default=pathlib.Path("output_data"))
parser.add_argument(
    "--shuffle", action="store_true", help="Randomise row order before export"
)
args = parser.parse_args()

args.out_dir.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("LOAD spatial;")


def _geom_col(path: pathlib.Path) -> str:
    rows = con.sql(f"DESCRIBE SELECT * FROM st_read('{path}')").fetchall()
    for row in rows:
        if "GEOMETRY" in row[1].upper():
            return row[0]
    raise ValueError(f"No geometry column found in {path}")


for src in args.sources:
    out = args.out_dir / f"{src.stem}_unoptimised.parquet"
    order_clause = "ORDER BY random()" if args.shuffle else ""
    if src.suffix.lower() == ".csv":
        print(f"Exporting {src.stem!r} (CSV) → {out} ...")
        con.execute(f"""
            COPY (
                SELECT * EXCLUDE (Longitude, Latitude),
                    ST_Point(Longitude::DOUBLE, Latitude::DOUBLE) AS geometry
                FROM read_csv('{src}')
                {order_clause}
            )
            TO '{out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    else:
        geom = _geom_col(src)
        print(f"Exporting {src.stem!r} ({geom!r}) → {out} ...")
        con.execute(f"""
            COPY (
                SELECT * EXCLUDE "{geom}", "{geom}" AS geometry
                FROM st_read('{src}')
                {order_clause}
            ) TO '{out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    print("  done.")
