import argparse
import logging
import pathlib
from typing import TYPE_CHECKING, ClassVar

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import sedona.db
import shapely

from . import bboxes as _bboxes
from .config import (
    DEFAULT_INPUT_DIR,
    DEFAULT_MATCHED_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_USRN_GPKG,
    BBox,
    DatasetConfig,
)
from .dtf import (
    DTFConfig,
    _build_dtf_gdf,
    to_dtf_csv,
    to_dtf_flat_csv,
    to_dtf_geoparquet,
    to_dtf_gpkg,
)
from .join import GeometryMode, JoinFn, run_intersect_join, run_nearest_join
from .logger import get_logger
from .prepare import prepare_dataset, prepare_from_csv, prepare_usrns

if TYPE_CHECKING:
    from sedonadb.context import SedonaContext

log: logging.Logger = get_logger()


class UsrnMatcher:
    """Spatially join USRNs to any polygon or point dataset using SedonaDB.

    Typical workflow::

        from usrn_matcher import UsrnMatcher, DatasetConfig

        cfg = DatasetConfig(
            name="highways",
            source_path="input_data/highways.gpkg",
            columns=["road_class", "speed_limit"],
        )

        matcher = UsrnMatcher(
            usrn_parquet="output_data/usrns_27700.parquet",
            rhs_config=cfg,
        )
        table = matcher.match_intersect()
        table = matcher.match_intersect(bbox=[412000, 426000, 444000, 445000])
        matcher.to_csv(table, "matched_data/usrn_highways_attribution.csv")
    """

    _JOIN_FNS: ClassVar[dict[str, JoinFn]] = {
        "intersect": run_intersect_join,
        "nearest": run_nearest_join,
    }
    _OUTPUT_FORMATS: ClassVar[dict[str, str]] = {
        "parquet": "to_parquet",
        "csv": "to_csv",
        "sample": "to_csv",
    }

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

    def _connect(self) -> "SedonaContext":
        if self._sd is None:
            self._sd = sedona.db.connect()
        assert self._sd is not None
        return self._sd

    def match_dispatch(
        self,
        mode: str,
        bbox: BBox | None = None,
        explain: bool = False,
        include_rhs_geometry: bool = False,
        usrn_batches: int = 1,
        distance_m: float = 50.0,
        geometry: GeometryMode = "none",
    ) -> pa.Table:
        """Dispatch to the registered JoinFn for the given mode."""
        if mode not in self._JOIN_FNS:
            raise ValueError(
                f"Unknown join mode {mode!r}. Available: {sorted(self._JOIN_FNS)}"
            )
        if bbox is None and usrn_batches == 1:
            usrn_batches = 10
        sd = self._connect()
        fn = self._JOIN_FNS[mode]
        result = fn(
            sd,
            self._usrn_parquet,
            self._rhs_config,
            bbox=bbox,
            explain=explain,
            include_rhs_geometry=include_rhs_geometry,
            usrn_batches=usrn_batches,
            distance_m=distance_m,
            geometry=geometry,
        )
        log.info("Result row count: %d", len(result))
        return result

    def match_intersect(
        self,
        bbox: BBox | None = None,
        explain: bool = False,
        include_rhs_geometry: bool = False,
        usrn_batches: int = 1,
        geometry: GeometryMode = "none",
    ) -> pa.Table:
        """Intersect join USRNs against the configured dataset."""
        return self.match_dispatch(
            "intersect",
            bbox=bbox,
            explain=explain,
            include_rhs_geometry=include_rhs_geometry,
            usrn_batches=usrn_batches,
            geometry=geometry,
        )

    def match_nearest(
        self,
        distance_m: float = 50.0,
        bbox: BBox | None = None,
        explain: bool = False,
        include_rhs_geometry: bool = False,
        usrn_batches: int = 1,
        geometry: GeometryMode = "none",
    ) -> pa.Table:
        """Find the nearest USRN for each point in the configured dataset."""
        return self.match_dispatch(
            "nearest",
            bbox=bbox,
            explain=explain,
            include_rhs_geometry=include_rhs_geometry,
            usrn_batches=usrn_batches,
            distance_m=distance_m,
            geometry=geometry,
        )

    def file_dispatch(
        self,
        table: pa.Table,
        output: str,
        matched_dir: pathlib.Path,
        stem: str,
        sample: int = 100_000,
    ) -> None:
        """Write match results to the requested output format."""
        if output not in self._OUTPUT_FORMATS:
            raise ValueError(
                f"Unknown output format {output!r}. Available: {sorted(self._OUTPUT_FORMATS)}"
            )
        match output:
            case "parquet":
                self.to_parquet(table, matched_dir / f"{stem}.parquet")
            case "csv":
                self.to_csv(table, matched_dir / f"{stem}.csv")
            case "sample":
                self.to_csv(table, matched_dir / f"{stem}_sample.csv", sample=sample)

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
            chunked: pa.ChunkedArray = (
                col.cast(col.type.storage_type)
                if hasattr(col.type, "storage_type")
                else col
            )
            raw = chunked.combine_chunks().to_numpy(zero_copy_only=False)
            return pa.array(shapely.to_wkt(shapely.from_wkb(raw)))

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
            Pre-process a spatial source file into an optimised GeoParquet.
        prepare-csv
            Pre-process a CSV with coordinate columns into optimised and GeoParquet.
        match
            Spatially join USRNs against a prepared dataset (spatial phase).
            Use ``--mode intersect`` (default) for polygon/line datasets, or
            ``--mode nearest`` for point datasets to find the closest USRN.
        """
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
        # prepare-usrns                                                        #
        # ------------------------------------------------------------------ #
        p_prepare_usrns = sub.add_parser(
            "prepare-usrns",
            help="Pre-process the OS Open USRN GeoPackage into optimised GeoParquet.",
        )
        p_prepare_usrns.add_argument(
            "--usrn-gpkg",
            default=DEFAULT_USRN_GPKG,
            metavar="PATH",
            help=f"Path to the OS Open USRN GeoPackage (default: {DEFAULT_USRN_GPKG}).",
        )
        p_prepare_usrns.add_argument(
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_prepare_usrns.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )

        # ------------------------------------------------------------------ #
        # prepare                                                              #
        # ------------------------------------------------------------------ #
        p_prepare = sub.add_parser(
            "prepare-gpkg",
            help="Pre-process source files into optimised GeoParquet (pre-spatial phase).",
        )
        p_prepare.add_argument(
            "--rhs-gpkg",
            default=None,
            metavar="PATH",
            help=f"Path to the RHS source file (default: {DEFAULT_INPUT_DIR}/{{rhs-name}}.gpkg).",
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
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
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
            default=None,
            metavar="PATH",
            help=f"Path to the source CSV file (default: {DEFAULT_INPUT_DIR}/{{name}}.csv).",
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
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
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
            choices=list(cls._OUTPUT_FORMATS),
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
            choices=list(cls._JOIN_FNS),
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
            "--geometry",
            choices=["none", "usrn", "clip", "rhs"],
            default="none",
            help=(
                "Geometry column in the output (default: none). "
                "'none' — attribute-only, fastest; "
                "'usrn' — full USRN line; "
                "'clip' — USRN clipped to matched polygon (intersect only, slower); "
                "'rhs' — matched RHS feature geometry."
            ),
        )
        p_match.add_argument(
            "--explain",
            action="store_true",
            help="Run EXPLAIN ANALYZE before the join (runs the join twice).",
        )
        p_match.add_argument(
            "--batches",
            type=int,
            default=1,
            metavar="N",
            help=(
                "Split the USRN parquet into N row-group batches and run the join once "
                "per batch (default: 1 = no batching). Use 4 for national joins to "
                "reduce peak memory and CPU pressure."
            ),
        )
        p_match.add_argument(
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory containing prepared GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_match.add_argument(
            "--matched-dir",
            default=DEFAULT_MATCHED_DIR,
            metavar="DIR",
            help=f"Directory for output files (default: {DEFAULT_MATCHED_DIR}).",
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
            choices=list(cls._JOIN_FNS),
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
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory containing prepared GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_export.add_argument(
            "--matched-dir",
            default=DEFAULT_MATCHED_DIR,
            metavar="DIR",
            help=f"Directory for output files (default: {DEFAULT_MATCHED_DIR}).",
        )
        p_export.add_argument(
            "--geometry",
            choices=["none", "usrn", "clip", "rhs"],
            default="none",
            help=(
                "Geometry column in the output (default: none). "
                "'none' — attribute-only, fastest; "
                "'usrn' — full USRN line; "
                "'clip' — USRN clipped to matched polygon (intersect only, slower); "
                "'rhs' — matched RHS feature geometry."
            ),
        )
        p_export.add_argument(
            "--explain",
            action="store_true",
            help="Run EXPLAIN ANALYZE before the join.",
        )
        p_export.add_argument(
            "--batches",
            type=int,
            default=1,
            metavar="N",
            help=(
                "Split the USRN parquet into N row-group batches (default: 1). "
                "Use 4 for national exports to reduce peak memory and CPU pressure."
            ),
        )

        args = parser.parse_args()

        # ------------------------------------------------------------------ #
        # Dispatch                                                           #
        # ------------------------------------------------------------------ #
        if args.command == "init":
            _cmd_init()

        elif args.command == "prepare-usrns":
            usrn_gpkg: pathlib.Path = pathlib.Path(args.usrn_gpkg)
            _validate_input_file(usrn_gpkg)
            cache_dir: pathlib.Path = pathlib.Path(args.cache_dir)
            prepare_usrns(
                usrn_gpkg,
                cache_dir / "usrns_27700.parquet",
                force=args.force,
            )

        elif args.command == "prepare-gpkg":
            cache_dir = pathlib.Path(args.cache_dir)
            rhs_gpkg: pathlib.Path = (
                pathlib.Path(args.rhs_gpkg)
                if args.rhs_gpkg is not None
                else DEFAULT_INPUT_DIR / f"{args.rhs_name}.gpkg"
            )
            _validate_input_file(rhs_gpkg)
            rhs_config: DatasetConfig = DatasetConfig(
                name=args.rhs_name,
                source_path=rhs_gpkg,
                parquet_path=cache_dir / f"{args.rhs_name}_27700.parquet",
                geometry_column=args.rhs_geometry_col,
                row_group_size=args.rhs_row_group_size,
            )
            prepare_dataset(rhs_config, force=args.force)

        elif args.command == "prepare-csv":
            csv_path: pathlib.Path = (
                pathlib.Path(args.csv)
                if args.csv is not None
                else DEFAULT_INPUT_DIR / f"{args.name}.csv"
            )
            _validate_input_file(csv_path)
            prepare_from_csv(
                csv_path=csv_path,
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
            bbox: BBox | None = (
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

            table: pa.Table = matcher.match_dispatch(
                mode=args.mode,
                bbox=bbox,
                explain=args.explain,
                usrn_batches=args.batches,
                distance_m=args.distance,
                geometry=args.geometry,
            )
            matcher.file_dispatch(
                table,
                output=args.output,
                matched_dir=matched_dir,
                stem=stem,
                sample=args.sample_rows,
            )

        elif args.command == "export":
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

            table = matcher.match_dispatch(
                mode=args.mode,
                bbox=bbox,
                explain=args.explain,
                include_rhs_geometry=True,
                usrn_batches=args.batches,
                distance_m=args.distance,
                geometry=args.geometry,
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


def _validate_input_file(path: pathlib.Path) -> None:
    """Check the input dir exists, all filenames are lowercase, and the target file is present."""
    if not DEFAULT_INPUT_DIR.exists():
        raise ValueError(
            f"Input directory '{DEFAULT_INPUT_DIR}' does not exist. Run 'usrn-matcher init' first."
        )

    bad: list[str] = [
        f.name
        for f in DEFAULT_INPUT_DIR.iterdir()
        if f.is_file() and f.name != f.name.lower()
    ]
    if bad:
        raise ValueError(
            f"All files in '{DEFAULT_INPUT_DIR}' must have lowercase names. "
            f"Rename: {', '.join(bad)}"
        )

    if not path.exists():
        raise ValueError(
            f"Input file '{path}' not found. "
            f"Place it in '{DEFAULT_INPUT_DIR}/' with a lowercase filename."
        )


def _cmd_init() -> None:
    """Create standard project directories if they don't exist."""
    dirs: list[pathlib.Path] = [
        DEFAULT_INPUT_DIR,
        DEFAULT_OUTPUT_DIR,
        DEFAULT_MATCHED_DIR,
    ]
    print("Initialising usrn-matcher project directories:")  # noqa: T201
    for d in dirs:
        created: bool = not d.exists()
        d.mkdir(exist_ok=True)
        status: str = "created" if created else "already exists"
        print(f"  {d}/  ({status})")  # noqa: T201
