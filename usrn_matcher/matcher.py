import logging
import pathlib
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import shapely

from .config import DatasetConfig
from .join import run_intersect_join, run_nearest_join
from .logger import get_logger
from .prepare import prepare_dataset, prepare_from_csv, prepare_usrns

if TYPE_CHECKING:
    from sedonadb.context import SedonaContext

log: logging.Logger = get_logger()


# TODO: Use match statements for the dispatch at the bottom
class UsrnMatcher:
    """Spatially join USRNs to any polygon or point dataset using SedonaDB.

    Typical workflow::

        from usrn_matcher import UsrnMatcher, DatasetConfig

        cfg = DatasetConfig(
            name="highways",
            source_path="input_data/highways.gpkg",
            columns=["road_class", "speed_limit"],
        )

        # Pre-spatial phase — run once, results cached as GeoParquet
        matcher = UsrnMatcher.from_sources(
            usrn_gpkg="input_data/osopenusrn.gpkg",
            rhs_config=cfg,
        )

        # Spatial phase — full national join or restricted to a bbox
        table = matcher.match_intersect()
        table = matcher.match_intersect(bbox=[412000, 426000, 444000, 445000])
        matcher.to_csv(table, "matched_data/usrn_highways_attribution.csv")

    If the GeoParquet files are already prepared::

        matcher = UsrnMatcher(
            usrn_parquet="output_data/usrns_27700.parquet",
            rhs_config=cfg,
        )
    """

    _usrn_parquet: pathlib.Path
    _rhs_config: DatasetConfig
    _sd: "SedonaContext | None"

    def __init__(
        self,
        usrn_parquet: str | pathlib.Path,
        rhs_config: DatasetConfig,
    ) -> None:
        self._usrn_parquet = pathlib.Path(usrn_parquet)
        self._rhs_config = rhs_config
        self._sd = None

    @classmethod
    def from_sources(
        cls,
        usrn_gpkg: str | pathlib.Path,
        rhs_config: DatasetConfig,
        cache_dir: str | pathlib.Path = "output_data",
        force_prepare: bool = False,
    ) -> "UsrnMatcher":
        """Build from source files, preparing GeoParquet caches as needed.

        Parameters
        ----------
        usrn_gpkg:
            Path to the OS Open USRN GeoPackage.
        rhs_config:
            Configuration for the right-hand side dataset.
            ``rhs_config.parquet_path`` controls where its GeoParquet is cached.
        cache_dir:
            Directory where the USRN GeoParquet is cached.
        force_prepare:
            Re-prepare GeoParquet files even if they already exist.
        """
        resolved_cache_dir: pathlib.Path = pathlib.Path(cache_dir)
        resolved_cache_dir.mkdir(parents=True, exist_ok=True)

        usrn_parquet: pathlib.Path = resolved_cache_dir / "usrns_27700.parquet"
        prepare_usrns(usrn_gpkg, usrn_parquet, force=force_prepare)
        prepare_dataset(rhs_config, force=force_prepare)

        return cls(usrn_parquet, rhs_config)

    def _connect(self) -> "SedonaContext":
        if self._sd is None:
            import sedona.db

            self._sd = sedona.db.connect()
        assert self._sd is not None
        return self._sd

    def match_intersect(
        self,
        bbox: list[float] | None = None,
        explain: bool = False,
        include_rhs_geometry: bool = False,
    ) -> pa.Table:
        """Spatially join USRNs to the configured dataset.

        Parameters
        ----------
        bbox:
            Optional ``[xmin, ymin, xmax, ymax]`` in EPSG:27700 (British
            National Grid metres). When ``None``, the full national join is
            executed — all USRNs are matched against all RHS geometries.
        explain:
            If ``True``, runs EXPLAIN ANALYZE first and logs the query plan.
            The join runs twice when this flag is set.
        include_rhs_geometry:
            If ``True``, appends a ``rhs_geometry`` column (WKB bytes of the
            matched RHS feature geometry). Required for DTF export.

        Returns
        -------
        pa.Table
            One row per USRN–RHS intersection. Geometry column contains WKB
            bytes with ``geoarrow.wkb`` extension type. When a bbox is given
            the geometries are clipped to its boundary.
        """
        sd = self._connect()
        table: pa.Table = run_intersect_join(
            sd,
            usrn_parquet=self._usrn_parquet,
            rhs_config=self._rhs_config,
            bbox=bbox,
            explain=explain,
            include_rhs_geometry=include_rhs_geometry,
        ).to_arrow_table()
        log.info("Result row count: %d", len(table))
        return table

    def match_nearest(
        self,
        distance_m: float = 50.0,
        bbox: list[float] | None = None,
        explain: bool = False,
        include_rhs_geometry: bool = False,
    ) -> pa.Table:
        """Find the nearest USRN for each point in the configured dataset.

        Parameters
        ----------
        distance_m:
            Search radius in metres. Only USRNs within this distance are
            considered. Default 50 m.
        bbox:
            Optional ``[xmin, ymin, xmax, ymax]`` in EPSG:27700 to restrict
            which points are matched.
        explain:
            If ``True``, runs EXPLAIN ANALYZE and logs the query plan.
        include_rhs_geometry:
            If ``True``, appends a ``rhs_geometry`` column (WKB bytes of the
            matched RHS point geometry). Required for DTF export.

        Returns
        -------
        pa.Table
            One row per input point — the nearest USRN, its street type, the
            clipped distance in metres, and all selected RHS columns.
        """
        sd = self._connect()
        result = run_nearest_join(
            sd,
            usrn_parquet=self._usrn_parquet,
            rhs_config=self._rhs_config,
            distance_m=distance_m,
            bbox=bbox,
            explain=explain,
            include_rhs_geometry=include_rhs_geometry,
        )

        return result.to_arrow_table()

    def to_parquet(self, table: pa.Table, path: str | pathlib.Path) -> None:
        """Write matched results as GeoParquet."""
        resolved_path: pathlib.Path = pathlib.Path(path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(resolved_path))
        log.info("Written %s", resolved_path)

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
            Result from :meth:`match_intersect`.
        path:
            Output file path.
        sample:
            If set, only write the first N rows.
        """
        resolved_path: pathlib.Path = pathlib.Path(path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)

        if sample is not None:
            table = table.slice(0, sample)

        def _wkb_col_to_wkt(col: pa.ChunkedArray) -> pa.Array:
            """Convert a WKB column (geoarrow or plain binary) to WKT strings."""
            raw: pa.ChunkedArray = (
                col.cast(col.type.storage_type)
                if hasattr(col.type, "storage_type")
                else col
            )
            return pa.array(shapely.to_wkt(shapely.from_wkb(raw.to_pylist())))

        # Convert the geometry column to WKT.
        for geom_field in ("geometry",):
            idx: int = table.schema.get_field_index(geom_field)
            if idx >= 0:
                table = table.set_column(
                    idx, geom_field, _wkb_col_to_wkt(table.column(geom_field))
                )

        # pyarrow CSV writer doesn't support string_view — cast to utf8
        # TODO: look into why this is the case
        new_schema: pa.Schema = pa.schema(
            [
                field.with_type(pa.utf8()) if field.type == pa.string_view() else field
                for field in table.schema
            ]
        )
        pcsv.write_csv(table.cast(new_schema), str(resolved_path))
        log.info("Written %s%s", resolved_path, f" ({sample} rows)" if sample else "")

    @classmethod
    def cli(cls) -> None:
        """Entry point for the ``usrn-matcher`` command-line tool.

        Sub-commands
        ------------
        init
            Create the standard project directories.
        prepare
            Pre-process a spatial source file into optimised GeoParquet.
        prepare-csv
            Pre-process a CSV with coordinate columns into optimised GeoParquet.
        match
            Spatially join USRNs against a prepared dataset (spatial phase).
            Use ``--mode intersect`` (default) for polygon/line datasets, or
            ``--mode nearest`` for point datasets to find the closest USRN.
        """
        import argparse

        from . import bboxes as _bboxes

        city_names: list[str] = [k for k in vars(_bboxes) if not k.startswith("_")]

        parser = argparse.ArgumentParser(
            prog="usrn-matcher",
            description="Spatially join USRNs to any spatial dataset.",
        )
        sub = parser.add_subparsers(dest="command", metavar="COMMAND")
        sub.required = True

        # ------------------------------------------------------------------ #
        # init                                                                 #
        # ------------------------------------------------------------------ #
        sub.add_parser(
            "init",
            help="Create project directories (input_data/, output_data/, matched_data/).",
        )

        # ------------------------------------------------------------------ #
        # prepare                                                              #
        # ------------------------------------------------------------------ #
        p_prepare = sub.add_parser(
            "prepare",
            help="Pre-process source files into optimised GeoParquet (pre-spatial phase).",
        )
        p_prepare.add_argument(
            "--usrn-gpkg",
            default="input_data/osopenusrn.gpkg",
            metavar="PATH",
            help="Path to the OS Open USRN GeoPackage (default: input_data/osopenusrn.gpkg).",
        )
        p_prepare.add_argument(
            "--rhs-gpkg",
            required=True,
            metavar="PATH",
            help="Path to the right-hand side source file (GeoPackage or any geopandas-readable format).",
        )
        p_prepare.add_argument(
            "--rhs-name",
            required=True,
            metavar="NAME",
            help="Short identifier for the RHS dataset (valid SQL identifier, e.g. 'soil', 'highways').",
        )
        p_prepare.add_argument(
            "--rhs-geometry-col",
            default="geometry",
            metavar="COL",
            help="Name of the geometry column in the RHS file (default: geometry).",
        )
        p_prepare.add_argument(
            "--rhs-row-group-size",
            type=int,
            default=10_000,
            metavar="N",
            help="Row group size for the RHS GeoParquet (default: 10000).",
        )
        p_prepare.add_argument(
            "--usrn-row-group-size",
            type=int,
            default=20_000,
            metavar="N",
            help="Row group size for the USRN GeoParquet (default: 20000).",
        )
        p_prepare.add_argument(
            "--cache-dir",
            default="output_data",
            metavar="DIR",
            help="Directory for cached GeoParquet files (default: output_data).",
        )
        p_prepare.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare GeoParquet files even if they already exist.",
        )

        # ------------------------------------------------------------------ #
        # prepare-csv                                                          #
        # ------------------------------------------------------------------ #
        p_prepare_csv = sub.add_parser(
            "prepare-csv",
            help="Pre-process a CSV with coordinate columns into optimised GeoParquet.",
        )
        p_prepare_csv.add_argument(
            "--csv",
            required=True,
            metavar="PATH",
            help="Path to the source CSV file.",
        )
        p_prepare_csv.add_argument(
            "--name",
            required=True,
            metavar="NAME",
            help="Short identifier for the dataset (valid SQL identifier, e.g. 'stops').",
        )
        p_prepare_csv.add_argument(
            "--x-col",
            default="Easting",
            metavar="COL",
            help="Column holding the X / Easting coordinate (default: Easting).",
        )
        p_prepare_csv.add_argument(
            "--y-col",
            default="Northing",
            metavar="COL",
            help="Column holding the Y / Northing coordinate (default: Northing).",
        )
        p_prepare_csv.add_argument(
            "--crs",
            default="EPSG:27700",
            metavar="CRS",
            help="CRS of the coordinate columns (default: EPSG:27700).",
        )
        p_prepare_csv.add_argument(
            "--row-group-size",
            type=int,
            default=10_000,
            metavar="N",
            help="Row group size for the output GeoParquet (default: 10000).",
        )
        p_prepare_csv.add_argument(
            "--cache-dir",
            default="output_data",
            metavar="DIR",
            help="Directory for cached GeoParquet files (default: output_data).",
        )
        p_prepare_csv.add_argument(
            "--geometry-type",
            default="point",
            metavar="TYPE",
            help="How to build geometries from the CSV (default: point).",
        )
        p_prepare_csv.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )

        # ------------------------------------------------------------------ #
        # match                                                                #
        # ------------------------------------------------------------------ #
        p_match = sub.add_parser(
            "match",
            help="Spatially join USRNs against a prepared dataset (spatial phase).",
        )
        p_match.add_argument(
            "--rhs-name",
            required=True,
            metavar="NAME",
            help="Name of the prepared RHS dataset (used to locate output_data/{name}_27700.parquet).",
        )
        p_match.add_argument(
            "--rhs-columns",
            nargs="*",
            default=[],
            metavar="COL",
            help="Columns to select from the RHS dataset. Omit to select all columns automatically.",
        )

        area = p_match.add_mutually_exclusive_group()
        area.add_argument(
            "--bbox",
            nargs=4,
            type=float,
            metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
            help="Bounding box in EPSG:27700. Omit for a full national join.",
        )
        area.add_argument(
            "--city",
            choices=city_names,
            metavar="CITY",
            help=f"Named city bbox. One of: {', '.join(city_names)}",
        )

        p_match.add_argument(
            "--output",
            choices=["parquet", "csv", "sample"],
            default="csv",
            help="Output format (default: csv).",
        )
        p_match.add_argument(
            "--sample-rows",
            type=int,
            default=100_000,
            help="Number of rows for --output sample (default: 100000).",
        )
        p_match.add_argument(
            "--mode",
            choices=["intersect", "nearest"],
            default="intersect",
            help=(
                "Join mode: 'intersect' for polygon/line datasets (default); "
                "'nearest' for point datasets — assigns each point to its closest USRN."
            ),
        )
        p_match.add_argument(
            "--distance",
            type=float,
            default=50.0,
            metavar="METRES",
            help="Search radius in metres for --mode nearest (default: 50).",
        )
        p_match.add_argument(
            "--explain",
            action="store_true",
            help="Run EXPLAIN ANALYZE before the join (runs the join twice).",
        )
        p_match.add_argument(
            "--cache-dir",
            default="output_data",
            metavar="DIR",
            help="Directory containing prepared GeoParquet files (default: output_data).",
        )
        p_match.add_argument(
            "--matched-dir",
            default="matched_data",
            metavar="DIR",
            help="Directory for output files (default: matched_data).",
        )

        # ------------------------------------------------------------------ #
        # export                                                               #
        # ------------------------------------------------------------------ #
        p_export = sub.add_parser(
            "export",
            help="Export join results as a DTF8.1-inspired CSV + GeoParquet.",
        )
        p_export.add_argument(
            "--rhs-name",
            required=True,
            metavar="NAME",
            help="Name of the prepared RHS dataset.",
        )
        p_export.add_argument(
            "--rhs-columns",
            nargs="*",
            default=[],
            metavar="COL",
            help="Columns to select from the RHS dataset. Omit to select all.",
        )

        export_area = p_export.add_mutually_exclusive_group()
        export_area.add_argument(
            "--bbox",
            nargs=4,
            type=float,
            metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
            help="Bounding box in EPSG:27700.",
        )
        export_area.add_argument(
            "--city",
            choices=city_names,
            metavar="CITY",
            help=f"Named city bbox. One of: {', '.join(city_names)}",
        )

        p_export.add_argument(
            "--mode",
            choices=["intersect", "nearest"],
            default="intersect",
            help="Join mode: 'intersect' (default) or 'nearest'.",
        )
        p_export.add_argument(
            "--distance",
            type=float,
            default=50.0,
            metavar="METRES",
            help="Search radius in metres for --mode nearest (default: 50).",
        )
        p_export.add_argument(
            "--dtf-org-name",
            default="usrn-matcher",
            metavar="NAME",
            help="Organisation name written to the DTF type 10 header (default: usrn-matcher).",
        )
        p_export.add_argument(
            "--dtf-org-ref",
            type=int,
            default=0,
            metavar="CODE",
            help="SWA organisation reference code for the DTF header (default: 0).",
        )
        p_export.add_argument(
            "--cache-dir",
            default="output_data",
            metavar="DIR",
            help="Directory containing prepared GeoParquet files (default: output_data).",
        )
        p_export.add_argument(
            "--matched-dir",
            default="matched_data",
            metavar="DIR",
            help="Directory for output files (default: matched_data).",
        )
        p_export.add_argument(
            "--explain",
            action="store_true",
            help="Run EXPLAIN ANALYZE before the join.",
        )

        args = parser.parse_args()

        # ------------------------------------------------------------------ #
        # Dispatch                                                           #
        # ------------------------------------------------------------------ #
        if args.command == "init":
            _cmd_init()

        elif args.command == "prepare":
            cache_dir: pathlib.Path = pathlib.Path(args.cache_dir)
            rhs_config: DatasetConfig = DatasetConfig(
                name=args.rhs_name,
                source_path=args.rhs_gpkg,
                parquet_path=cache_dir / f"{args.rhs_name}_27700.parquet",
                geometry_column=args.rhs_geometry_col,
                row_group_size=args.rhs_row_group_size,
            )
            prepare_usrns(
                args.usrn_gpkg,
                cache_dir / "usrns_27700.parquet",
                force=args.force,
            )
            prepare_dataset(rhs_config, force=args.force)

        elif args.command == "prepare-csv":
            prepare_from_csv(
                csv_path=args.csv,
                parquet_path=pathlib.Path(args.cache_dir)
                / f"{args.name}_27700.parquet",
                geometry_type=args.geometry_type,
                x_col=args.x_col,
                y_col=args.y_col,
                crs=args.crs,
                row_group_size=args.row_group_size,
                force=args.force,
            )

        elif args.command == "match":
            cache_dir = pathlib.Path(args.cache_dir)
            rhs_config = DatasetConfig(
                name=args.rhs_name,
                source_path=cache_dir / f"{args.rhs_name}_27700.parquet",
                parquet_path=cache_dir / f"{args.rhs_name}_27700.parquet",
                columns=args.rhs_columns,
            )
            bbox: list[float] | None = (
                args.bbox
                if args.bbox is not None
                else (getattr(_bboxes, args.city) if args.city else None)
            )
            matcher: UsrnMatcher = cls(
                usrn_parquet=cache_dir / "usrns_27700.parquet",
                rhs_config=rhs_config,
            )

            stem: str = f"usrn_{args.rhs_name}_attribution"
            matched_dir: pathlib.Path = pathlib.Path(args.matched_dir)

            if args.mode == "nearest":
                table: pa.Table = matcher.match_nearest(
                    distance_m=args.distance,
                    bbox=bbox,
                    explain=args.explain,
                )
                stem = f"usrn_{args.rhs_name}_attribution"
            else:
                table = matcher.match_intersect(bbox=bbox, explain=args.explain)

            if args.output == "parquet":
                matcher.to_parquet(table, matched_dir / f"{stem}.parquet")
            elif args.output == "csv":
                matcher.to_csv(table, matched_dir / f"{stem}.csv")
            elif args.output == "sample":
                matcher.to_csv(
                    table,
                    matched_dir / f"{stem}_sample.csv",
                    sample=args.sample_rows,
                )

        elif args.command == "export":
            from .dtf import (
                DTFConfig,
                _build_dtf_gdf,
                to_dtf_csv,
                to_dtf_flat_csv,
                to_dtf_geoparquet,
                to_dtf_gpkg,
            )

            cache_dir = pathlib.Path(args.cache_dir)
            rhs_config = DatasetConfig(
                name=args.rhs_name,
                source_path=cache_dir / f"{args.rhs_name}_27700.parquet",
                parquet_path=cache_dir / f"{args.rhs_name}_27700.parquet",
                columns=args.rhs_columns,
            )
            bbox = (
                args.bbox
                if args.bbox is not None
                else (getattr(_bboxes, args.city) if args.city else None)
            )
            matcher = cls(
                usrn_parquet=cache_dir / "usrns_27700.parquet",
                rhs_config=rhs_config,
            )

            if args.mode == "nearest":
                table = matcher.match_nearest(
                    distance_m=args.distance,
                    bbox=bbox,
                    explain=args.explain,
                    include_rhs_geometry=True,
                )
            else:
                table = matcher.match_intersect(
                    bbox=bbox,
                    explain=args.explain,
                    include_rhs_geometry=True,
                )

            dtf_config = DTFConfig(
                swa_org_name=args.dtf_org_name,
                swa_org_ref=args.dtf_org_ref,
                rhs_name=args.rhs_name,
            )
            matched_dir = pathlib.Path(args.matched_dir)
            stem = f"matched_{args.rhs_name}_ad"
            # Build the sorted GDF once — shared by the three geometry-bearing writers
            # so the Hilbert sort only runs once instead of once per output format.
            gdf = _build_dtf_gdf(table, dtf_config)
            to_dtf_csv(table, dtf_config, matched_dir / f"{stem}.csv")
            to_dtf_geoparquet(
                table, dtf_config, matched_dir / f"{stem}.parquet", _gdf=gdf
            )
            to_dtf_flat_csv(
                table, dtf_config, matched_dir / f"{stem}_flat.csv", _gdf=gdf
            )
            to_dtf_gpkg(table, dtf_config, matched_dir / f"{stem}.gpkg", _gdf=gdf)


def _cmd_init() -> None:
    """Create standard project directories if they don't exist."""
    dirs: list[pathlib.Path] = [
        pathlib.Path("input_data"),
        pathlib.Path("output_data"),
        pathlib.Path("matched_data"),
    ]
    print("Initialising usrn-matcher project directories:")
    for d in dirs:
        created: bool = not d.exists()
        d.mkdir(exist_ok=True)
        status: str = "created" if created else "already exists"
        print(f"  {d}/  ({status})")
