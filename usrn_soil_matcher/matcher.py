import pathlib

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import shapely

from .convert import convert_gpkgs
from .join import run_join
from .logger import get_logger

log = get_logger()


class UsrnSoilMatcher:
    """Spatially join USRNs to National Soil Map polygons using SedonaDB.

    Usage:

        matcher = UsrnSoilMatcher.from_gpkgs(
            usrn_gpkg="input_data/osopenusrn.gpkg",
            soil_gpkg="input_data/NationalSoilMap.gpkg",
        )
        table = matcher.match(bbox=[412000, 426000, 444000, 445000])
        matcher.to_csv(table, "matched_data/output.csv")

    Or if you already have GeoParquet files:

        matcher = UsrnSoilMatcher(
            usrn_parquet="output_data/usrns_27700.parquet",
            soil_parquet="output_data/soil_27700.parquet",
        )
        table = matcher.match(bbox=[503000, 155000, 562000, 200000])
    """

    def __init__(
        self, usrn_parquet: str | pathlib.Path, soil_parquet: str | pathlib.Path
    ):
        self._usrn_parquet = pathlib.Path(usrn_parquet)
        self._soil_parquet = pathlib.Path(soil_parquet)
        self._sd = None

    @classmethod
    def from_gpkgs(
        cls,
        usrn_gpkg: str | pathlib.Path,
        soil_gpkg: str | pathlib.Path,
        cache_dir: str | pathlib.Path = "output_data",
    ) -> "UsrnSoilMatcher":
        """Build from source GeoPackages, converting to GeoParquet if needed.

        Converted files are written to ``cache_dir`` and reused on subsequent
        calls — delete them to force re-conversion.
        """
        cache_dir = pathlib.Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)

        usrn_parquet = cache_dir / "usrns_27700.parquet"
        soil_parquet = cache_dir / "soil_27700.parquet"

        if not usrn_parquet.exists() or not soil_parquet.exists():
            convert_gpkgs(
                usrn_gpkg=pathlib.Path(usrn_gpkg),
                soil_gpkg=pathlib.Path(soil_gpkg),
                usrn_parquet=usrn_parquet,
                soil_parquet=soil_parquet,
            )
        else:
            log.info(
                "GeoParquets already exist — skipping conversion. Delete them to re-convert."
            )

        return cls(usrn_parquet, soil_parquet)

    def _connect(self):
        if self._sd is None:
            import sedona.db

            self._sd = sedona.db.connect()
        return self._sd

    def match(
        self,
        bbox: list[float],
        explain: bool = False,
    ) -> pa.Table:
        """Spatially join USRNs to soil polygons within a bounding box.

        Parameters
        ----------
        bbox:
            ``[xmin, ymin, xmax, ymax]`` in EPSG:27700 (British National Grid metres).
        explain:
            If True, runs EXPLAIN ANALYZE first and logs the query plan.

        Returns
        -------
        pa.Table
            One row per USRN–soil intersection, clipped to the bbox boundary.
            Geometry column contains WKB bytes with ``geoarrow.wkb`` extension type.
        """
        sd = self._connect()
        result = run_join(
            sd,
            usrn_parquet=self._usrn_parquet,
            soil_parquet=self._soil_parquet,
            bbox=bbox,
            explain=explain,
        )
        return result.to_arrow_table()

    def to_parquet(self, table: pa.Table, path: str | pathlib.Path) -> None:
        """Write matched results as GeoParquet."""
        path = pathlib.Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(path))
        log.info("Written %s", path)

    def to_csv(
        self,
        table: pa.Table,
        path: str | pathlib.Path,
        sample: int | None = None,
    ) -> None:
        """Write matched results as CSV with a WKT geometry column.

        Parameters
        ----------
        table:
            Result from :meth:`match`.
        path:
            Output file path.
        sample:
            If set, only write the first N rows.
        """
        path = pathlib.Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if sample is not None:
            table = table.slice(0, sample)

        # Convert geoarrow.wkb → WKT strings via shapely
        geom_col = table.column("geometry")
        raw_wkb = geom_col.cast(geom_col.type.storage_type)
        wkt = pa.array(shapely.to_wkt(shapely.from_wkb(raw_wkb.to_pylist())))
        geom_idx = table.schema.get_field_index("geometry")
        table = table.set_column(geom_idx, "geometry", wkt)

        # pyarrow CSV writer doesn't support string_view — cast to utf8
        # TODO: look into why this is the case
        new_schema = pa.schema(
            [
                field.with_type(pa.utf8()) if field.type == pa.string_view() else field
                for field in table.schema
            ]
        )
        pcsv.write_csv(table.cast(new_schema), str(path))
        log.info("Written %s%s", path, f" ({sample} rows)" if sample else "")

    @classmethod
    def cli(cls) -> None:
        """Entry point for the command-line interface."""
        import argparse

        from .constants import SOIL_GPKG, USRN_GPKG

        parser = argparse.ArgumentParser(
            description="Spatially join USRNs to soil polygons."
        )
        parser.add_argument(
            "--output",
            choices=["parquet", "csv", "sample"],
            default="csv",
            help="Output format: parquet (GeoParquet), csv (full CSV with WKT), sample (first N rows as CSV)",
        )
        parser.add_argument(
            "--sample-rows",
            type=int,
            default=100_000,
            help="Number of rows for --output sample (default: 100000)",
        )
        parser.add_argument(
            "--explain",
            action="store_true",
            help="Run EXPLAIN ANALYZE before the join (runs the join twice).",
        )
        from . import bboxes as _bboxes

        city_names = [k for k in vars(_bboxes) if not k.startswith("_")]

        area = parser.add_mutually_exclusive_group(required=True)
        area.add_argument(
            "--bbox",
            nargs=4,
            type=float,
            metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
            help="Bounding box in EPSG:27700. E.g. 503000 155000 562000 200000",
        )
        area.add_argument(
            "--city",
            choices=city_names,
            metavar="CITY",
            help=f"Named city bbox. One of: {', '.join(city_names)}",
        )
        args = parser.parse_args()

        bbox = args.bbox if args.bbox is not None else getattr(_bboxes, args.city)

        matcher = cls.from_gpkgs(usrn_gpkg=USRN_GPKG, soil_gpkg=SOIL_GPKG)
        table = matcher.match(bbox=bbox, explain=args.explain)

        if args.output == "parquet":
            matcher.to_parquet(table, "matched_data/usrn_soil_attribution.parquet")
        elif args.output == "csv":
            matcher.to_csv(table, "matched_data/usrn_soil_attribution.csv")
        elif args.output == "sample":
            matcher.to_csv(
                table,
                "matched_data/usrn_soil_attribution_sample.csv",
                sample=args.sample_rows,
            )
