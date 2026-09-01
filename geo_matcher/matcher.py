import argparse
import logging
import pathlib
from typing import TYPE_CHECKING, ClassVar

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import sedona.db

from . import bboxes as _bboxes
from .config import (
    DEFAULT_INPUT_DIR,
    DEFAULT_MATCHED_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_UPRN_GPKG,
    DEFAULT_USRN_GPKG,
    BBox,
    CsvSource,
    DatasetConfig,
    GeometryType,
    LhsKind,
    MatchSource,
    OgrSource,
    ParquetSource,
    UprnSource,
    UsrnSource,
)
from .join import (
    FilteredMode,
    JoinFn,
    JoinMode,
    NationalMode,
    _registry,
    configure_sedona_session,
    get_join,
)
from .logger import get_logger
from .prepare import prepare

if TYPE_CHECKING:
    from sedonadb.context import SedonaContext

log: logging.Logger = get_logger()


class GeoMatcher:
    """Spatially join USRNs to any polygon, point, or linestring dataset using SedonaDB."""

    _OUTPUT_FORMATS: ClassVar[dict[str, str]] = {
        "parquet": "_to_parquet",
        "csv": "_to_csv",
        "sample": "_to_csv",
    }

    _usrn_parquet: pathlib.Path
    _rhs_config: DatasetConfig
    _sd: "SedonaContext | None"
    _threads: (
        int | None
    )  # This limits the amount of CPU/threads the SeondaSession has access to

    def __init__(
        self,
        usrn_parquet: str | pathlib.Path,
        rhs_config: DatasetConfig,
        *,
        threads: int | None = None,
    ) -> None:
        self._usrn_parquet = pathlib.Path(usrn_parquet)
        self._rhs_config = rhs_config
        self._sd = None
        self._threads = threads

    def _connect(self) -> "SedonaContext":
        if self._sd is None:
            sd: SedonaContext = sedona.db.connect()
            configure_sedona_session(sd, target_partitions=self._threads or 4)
            self._sd = sd
        assert self._sd is not None
        return self._sd

    def match_dispatch(
        self,
        mode: GeometryType | str,
        lhs: LhsKind | str = LhsKind.USRN,
        bbox: BBox | None = None,
        explain: bool = False,
        n_chunks: int = 50,
        distance_m: float = 10.0,
        phase3_distance_m: float | None = None,
        rhs_id_col: str | None = None,
        output_path: pathlib.Path | None = None,
        overlap_threshold: float = 0.10,
        usrn_line_parquet: pathlib.Path | None = None,
        phase4_tolerance_m: float = 5.0,
    ) -> pa.Table:
        """Dispatch the match to the registered Join Function.

        *mode* is the RHS geometry type and *lhs* is the base dataset to join
        from (``"usrn"`` street centrelines by default, or ``"uprn"`` address
        points) — the pair must have a join registered against it.

        The Analysis mode is driven by whether or not a bbox is provided.
        """
        try:
            # This will be one of the registered JoinFns
            # from join.py such as run_usrn_line_join
            geometry_type: GeometryType = GeometryType(mode)
            lhs_kind: LhsKind = LhsKind(lhs)
            fn: JoinFn = get_join(lhs_kind, geometry_type)
        except (ValueError, KeyError):
            raise ValueError(
                f"Unknown join lhs={lhs!r}/mode={mode!r}. "
                f"Available: {[(str(lk), str(gk)) for (lk, gk) in _registry]}"
            ) from None
        join_mode: JoinMode = (
            FilteredMode(bbox=bbox)
            if bbox is not None
            else NationalMode(n_chunks=n_chunks)
        )
        sd: SedonaContext = self._connect()
        result: pa.Table = fn(
            sd,
            usrn_parquet=self._usrn_parquet,
            rhs_config=self._rhs_config,
            mode=join_mode,
            explain=explain,
            distance_m=distance_m,
            phase3_distance_m=phase3_distance_m,
            rhs_id_col=rhs_id_col,
            output_path=output_path,
            overlap_threshold=overlap_threshold,
            usrn_line_parquet=usrn_line_parquet,
            phase4_tolerance_m=phase4_tolerance_m,
        )
        if output_path is not None and output_path.exists():
            log.info(
                "Result row count: %d (streamed to %s)",
                pq.read_metadata(str(output_path)).num_rows,
                output_path,
            )
        else:
            log.info("Result row count: %d", len(result))
        return result

    def output_writer(
        self,
        table: pa.Table,
        output: str,
        matched_dir: pathlib.Path,
        stem: str,
        sample: int = 100_000,
    ) -> None:
        """Write finished match results to the requested output format.

        Takes in a Arrow table and outputs a parquet file (or csv)"""
        if output not in self._OUTPUT_FORMATS:
            raise ValueError(
                f"Unknown output format {output!r}. Available: {sorted(self._OUTPUT_FORMATS)}"
            )
        match output:
            case "parquet":
                self._to_parquet(table, matched_dir / f"{stem}.parquet")
            case "csv":
                self._to_csv(table, matched_dir / f"{stem}.csv")
            case "sample":
                self._to_csv(table, matched_dir / f"{stem}_sample.csv", sample=sample)

    def _to_parquet(self, table: pa.Table, path: str | pathlib.Path) -> None:
        """Write match results as Parquet (attribute-only, no geometry column)."""
        resolved_path: pathlib.Path = pathlib.Path(path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(resolved_path))
        log.info("Written %s", resolved_path)

    def _to_csv(
        self,
        table: pa.Table,
        path: str | pathlib.Path,
        sample: int | None = None,
    ) -> None:
        """Write matched results as CSV.

        If sample arg defined then a sample is the ouput
        based on the defined range.
        """
        resolved_path: pathlib.Path = pathlib.Path(path)
        resolved_path.parent.mkdir(parents=True, exist_ok=True)

        slice_offset: int | None = None
        slice_length: int | None = None

        # Ouput as sample if a range is defined
        if sample is not None:
            slice_offset = 0
            slice_length = sample
            table = table.slice(slice_offset, slice_length)
            log.info(
                "CSV sample slice for %s: table.slice(%d, %d) -> %d rows",
                resolved_path,
                slice_offset,
                slice_length,
                len(table),
            )

        # pyarrow CSV writer doesn't support string_view — cast to utf8
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
        """Entry point for the ``geo-matcher`` command-line tool."""
        city_names: list[str] = [
            city for city in vars(_bboxes) if not city.startswith("_")
        ]

        parser = argparse.ArgumentParser(
            prog="geo-matcher",
            description="Spatially join USRNs to any spatial dataset.",
        )
        sub = parser.add_subparsers(dest="command", metavar="COMMAND")
        sub.required = True

        # ------------------------------------------------------------------ #
        # init                                                               #
        # ------------------------------------------------------------------ #
        sub.add_parser(
            "init",
            help="Create project directories (input_data/, output_data/, matched_data/).",
        )

        # ------------------------------------------------------------------ #
        # prepare-usrns                                                      #
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
        p_prepare_usrns.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # prepare-usrns-line                                                 #
        # ------------------------------------------------------------------ #
        p_prepare_usrns_line = sub.add_parser(
            "prepare-usrns-line",
            help="Prepare a buffered USRN GeoParquet for line joins.",
        )
        p_prepare_usrns_line.add_argument(
            "--buffer-m",
            type=float,
            required=True,
            metavar="METRES",
            help="Buffer radius in metres applied to each USRN centreline.",
        )
        p_prepare_usrns_line.add_argument(
            "--usrn-parquet",
            default=None,
            metavar="PATH",
            help="Path to existing usrns_27700.parquet (default: {cache-dir}/usrns_27700.parquet).",
        )
        p_prepare_usrns_line.add_argument(
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_prepare_usrns_line.add_argument(
            "--row-group-size",
            type=int,
            default=20_000,
            metavar="N",
            help="Row group size for the output GeoParquet (default: 20000).",
        )
        p_prepare_usrns_line.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )
        p_prepare_usrns_line.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # prepare-uprns                                                      #
        # ------------------------------------------------------------------ #
        p_prepare_uprns = sub.add_parser(
            "prepare-uprns",
            help="Pre-process the OS Open UPRN GeoPackage into optimised GeoParquet.",
        )
        p_prepare_uprns.add_argument(
            "--uprn-gpkg",
            default=DEFAULT_UPRN_GPKG,
            metavar="PATH",
            help=f"Path to the OS Open UPRN GeoPackage (default: {DEFAULT_UPRN_GPKG}).",
        )
        p_prepare_uprns.add_argument(
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_prepare_uprns.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )
        p_prepare_uprns.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # prepare-uprns-buffer                                               #
        # ------------------------------------------------------------------ #
        p_prepare_uprns_buffer = sub.add_parser(
            "prepare-uprns-buffer",
            help="Prepare buffered UPRN catchment polygons from a prepared UPRN GeoParquet.",
        )
        p_prepare_uprns_buffer.add_argument(
            "--buffer-m",
            type=float,
            required=True,
            metavar="METRES",
            help="Buffer radius in metres applied to each UPRN point.",
        )
        p_prepare_uprns_buffer.add_argument(
            "--uprn-parquet",
            default=None,
            metavar="PATH",
            help="Path to existing uprns_27700.parquet (default: {cache-dir}/uprns_27700.parquet).",
        )
        p_prepare_uprns_buffer.add_argument(
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_prepare_uprns_buffer.add_argument(
            "--row-group-size",
            type=int,
            default=20_000,
            metavar="N",
            help="Row group size for the output GeoParquet (default: 20000).",
        )
        p_prepare_uprns_buffer.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )
        p_prepare_uprns_buffer.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # prepare                                                            #
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
        p_prepare.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # prepare-csv                                                        #
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
            "--wkt-col",
            default=None,
            metavar="COL",
            help=(
                "Column holding WKT geometry text (e.g. 'LINESTRING(...)' or "
                "'POLYGON(...)'). Required when --geometry-type is 'line' or "
                "'polygon'; unused for 'point'."
            ),
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
            choices=[g.value for g in GeometryType],
            default=GeometryType.POINT.value,
            help=(
                "How to build geometries from the CSV (default: point). "
                "'point' uses --x-col/--y-col; 'line' and 'polygon' require "
                "--wkt-col holding WKT text (LINESTRING/MULTILINESTRING or "
                "POLYGON/MULTIPOLYGON)."
            ),
        )
        p_prepare_csv.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )
        p_prepare_csv.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # prepare-parquet                                                    #
        # ------------------------------------------------------------------ #
        p_prepare_parquet = sub.add_parser(
            "prepare-parquet",
            help="Re-sort and re-compress an existing GeoParquet into the optimised format.",
        )
        p_prepare_parquet.add_argument(
            "--parquet",
            default=None,
            metavar="PATH",
            help=f"Path to the source GeoParquet file (default: {DEFAULT_INPUT_DIR}/{{name}}.parquet).",
        )
        p_prepare_parquet.add_argument(
            "--name",
            required=True,
            metavar="NAME",
            help="Short identifier for the dataset (valid SQL identifier, e.g. 'highways').",
        )
        p_prepare_parquet.add_argument(
            "--crs",
            default="EPSG:27700",
            metavar="CRS",
            help="CRS to record in the output GeoParquet metadata (default: EPSG:27700).",
        )
        p_prepare_parquet.add_argument(
            "--row-group-size",
            type=int,
            default=10_000,
            metavar="N",
            help="Row group size for the output GeoParquet (default: 10000).",
        )
        p_prepare_parquet.add_argument(
            "--cache-dir",
            default=DEFAULT_OUTPUT_DIR,
            metavar="DIR",
            help=f"Directory for cached GeoParquet files (default: {DEFAULT_OUTPUT_DIR}).",
        )
        p_prepare_parquet.add_argument(
            "--geometry-col",
            default="geometry",
            metavar="COL",
            help=(
                "Name of the geometry column in the source file (default: geometry). "
                "For external files set this to the actual column name, e.g. 'geo_shape'."
            ),
        )
        p_prepare_parquet.add_argument(
            "--source-crs",
            default=None,
            metavar="CRS",
            help=(
                "CRS of the source geometry column, e.g. 'EPSG:4326'. "
                "Required when the source column is in a different CRS than --crs. "
                "The geometry will be reprojected to --crs during prepare."
            ),
        )
        p_prepare_parquet.add_argument(
            "--force",
            action="store_true",
            help="Re-prepare even if the GeoParquet already exists.",
        )
        p_prepare_parquet.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help="DuckDB thread count (default: all cores). Lower to reduce CPU pressure.",
        )

        # ------------------------------------------------------------------ #
        # match                                                              #
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
            "--lhs-name",
            choices=[lhs.value for lhs in LhsKind],
            default=LhsKind.USRN.value,
            help=(
                "Base dataset to join from (default: 'usrn'): 'usrn' — street "
                "centrelines; 'uprn' — address points. Not every --mode is "
                "registered for every --lhs-name (currently 'uprn' only has 'polygon')."
            ),
        )
        p_match.add_argument(
            "--mode",
            choices=sorted({g.value for (_, g) in _registry}),
            default=GeometryType.POLYGON.value,
            help=(
                "Join strategy: 'polygon' for area/polygon datasets (default); "
                "'point' for point datasets — assigns each point to its closest USRN; "
                "'line' for linestring datasets — two-phase corridor match, requires --usrn-line-parquet."
            ),
        )
        p_match.add_argument(
            "--distance",
            type=float,
            default=10.0,
            metavar="METRES",
            help="Tolerance/search radius in metres for --mode line and --mode point (default: 10).",
        )
        p_match.add_argument(
            "--phase3-distance",
            type=float,
            default=None,
            metavar="METRES",
            help=(
                "ST_DWithin search radius for --mode line Phase 3 nearest fallback (default: same as --distance). "
                "Increasing this catches features that run just outside the Phase 2 corridor."
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
            default=50,
            metavar="N",
            help=(
                "Number of RHS chunks for national (no-bbox) joins (default: 50). "
                "The RHS parquet is split into this many in-memory chunks of row "
                "groups; only one chunk's data is in memory at a time. Ignored when "
                "--bbox or --city is supplied. (Line joins further split each chunk "
                "into 5,000-row batches for Phases 3 and 4.)"
            ),
        )
        p_match.add_argument(
            "--rhs-id-col",
            default=None,
            metavar="COL",
            help=(
                "Column that uniquely identifies each RHS feature (e.g. 'ASSET_ID'). "
                "Required for --mode line."
            ),
        )
        p_match.add_argument(
            "--overlap-threshold",
            type=float,
            default=0.10,
            metavar="FRAC",
            help=(
                "Minimum overlap fraction for --mode line Phase 2 corridor matches (default: 0.10). "
                "At least this fraction of the RHS line must fall inside the USRN buffer "
                "corridor to be kept. Features below the threshold are dropped entirely."
            ),
        )
        p_match.add_argument(
            "--phase4-tolerance",
            type=float,
            default=5.0,
            metavar="METRES",
            help=(
                "Connection tolerance in metres for --mode line Phase 4 (default: 5). "
                "A feature still unmatched after Phase 3 inherits the USRN of an "
                "already-matched feature it physically touches within this distance. "
                "Set to 0 to disable Phase 4."
            ),
        )
        p_match.add_argument(
            "--threads",
            type=int,
            default=None,
            metavar="N",
            help=(
                "DataFusion target_partitions for the spatial join (default: 4). "
                "Lower values reduce CPU saturation at the cost of longer wall time. "
                "Use 1-2 to run in the background without impacting other processes."
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
        p_match.add_argument(
            "--usrn-line-parquet",
            default=None,
            metavar="PATH",
            help="Path to the buffered USRN GeoParquet for line join Phase 2 (prepared via prepare-usrns-line).",
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
            prepare(
                DatasetConfig(
                    name="usrns",
                    source=UsrnSource(path=usrn_gpkg, row_group_size=20_000),
                    parquet_path=cache_dir / "usrns_27700.parquet",
                ),
                force=args.force,
                threads=args.threads,
            )

        elif args.command == "prepare-usrns-line":
            cache_dir = pathlib.Path(args.cache_dir)
            usrn_parquet_path: pathlib.Path = (
                pathlib.Path(args.usrn_parquet)
                if args.usrn_parquet
                else cache_dir / "usrns_27700.parquet"
            )
            buffer_m: float = args.buffer_m
            buffer_label: str = f"{buffer_m:g}".replace(".", "_")
            prepare(
                DatasetConfig(
                    name=f"usrns_line_{buffer_label}m",
                    source=UsrnSource(
                        path=usrn_parquet_path,
                        buffer_m=buffer_m,
                        row_group_size=args.row_group_size,
                    ),
                    parquet_path=cache_dir
                    / f"usrns_line_{buffer_label}m_27700.parquet",
                ),
                force=args.force,
                threads=args.threads,
            )

        elif args.command == "prepare-uprns":
            uprn_gpkg: pathlib.Path = pathlib.Path(args.uprn_gpkg)
            _validate_input_file(uprn_gpkg)
            cache_dir = pathlib.Path(args.cache_dir)
            prepare(
                DatasetConfig(
                    name="uprns",
                    source=UprnSource(path=uprn_gpkg, row_group_size=20_000),
                    parquet_path=cache_dir / "uprns_27700.parquet",
                ),
                force=args.force,
                threads=args.threads,
            )

        elif args.command == "prepare-uprns-buffer":
            cache_dir = pathlib.Path(args.cache_dir)
            uprn_parquet_path: pathlib.Path = (
                pathlib.Path(args.uprn_parquet)
                if args.uprn_parquet
                else cache_dir / "uprns_27700.parquet"
            )
            buffer_m = args.buffer_m
            buffer_label = f"{buffer_m:g}".replace(".", "_")
            prepare(
                DatasetConfig(
                    name=f"uprns_buffer_{buffer_label}m",
                    source=UprnSource(
                        path=uprn_parquet_path,
                        buffer_m=buffer_m,
                        row_group_size=args.row_group_size,
                    ),
                    parquet_path=cache_dir
                    / f"uprns_buffer_{buffer_label}m_27700.parquet",
                ),
                force=args.force,
                threads=args.threads,
            )

        elif args.command == "prepare-gpkg":
            cache_dir = pathlib.Path(args.cache_dir)
            rhs_gpkg: pathlib.Path = (
                pathlib.Path(args.rhs_gpkg)
                if args.rhs_gpkg is not None
                else DEFAULT_INPUT_DIR / f"{args.rhs_name}.gpkg"
            )
            _validate_input_file(rhs_gpkg)
            match_source: MatchSource = OgrSource(
                path=rhs_gpkg,
                row_group_size=args.rhs_row_group_size,
            )
            rhs_config: DatasetConfig = DatasetConfig(
                name=args.rhs_name,
                source=match_source,
                parquet_path=cache_dir / f"{args.rhs_name}_27700.parquet",
            )
            prepare(rhs_config, force=args.force, threads=args.threads)

        elif args.command == "prepare-csv":
            csv_path: pathlib.Path = (
                pathlib.Path(args.csv)
                if args.csv is not None
                else DEFAULT_INPUT_DIR / f"{args.name}.csv"
            )
            _validate_input_file(csv_path)
            match_source = CsvSource(
                path=csv_path,
                x_col=args.x_col,
                y_col=args.y_col,
                geometry_type=args.geometry_type,
                wkt_col=args.wkt_col,
                crs=args.crs,
                row_group_size=args.row_group_size,
            )
            rhs_config = DatasetConfig(
                name=args.name,
                source=match_source,
                parquet_path=pathlib.Path(args.cache_dir)
                / f"{args.name}_27700.parquet",
            )
            prepare(rhs_config, force=args.force, threads=args.threads)

        elif args.command == "prepare-parquet":
            src_parquet: pathlib.Path = (
                pathlib.Path(args.parquet)
                if args.parquet is not None
                else DEFAULT_INPUT_DIR / f"{args.name}.parquet"
            )
            _validate_input_file(src_parquet)
            match_source = ParquetSource(
                path=src_parquet,
                crs=args.crs,
                row_group_size=args.row_group_size,
                geometry_col=args.geometry_col,
                source_crs=args.source_crs,
            )
            rhs_config = DatasetConfig(
                name=args.name,
                source=match_source,
                parquet_path=pathlib.Path(args.cache_dir)
                / f"{args.name}_27700.parquet",
            )
            prepare(rhs_config, force=args.force, threads=args.threads)

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
            if bbox is None and args.batches < 2:
                parser.error("--batches must be >= 2 for national (no-bbox) joins")
            if args.mode == GeometryType.LINE and args.usrn_line_parquet is None:
                parser.error(
                    "--mode line requires --usrn-line-parquet (run 'prepare-usrns-line --buffer-m N' first)"
                )

            lhs_parquet_name: str = (
                "usrns_27700.parquet"
                if args.lhs_name == LhsKind.USRN
                else "uprns_27700.parquet"
            )
            matcher: GeoMatcher = cls(
                usrn_parquet=cache_dir / lhs_parquet_name,
                rhs_config=rhs_config,
                threads=args.threads,
            )

            stem: str = f"{args.lhs_name}_{args.rhs_name}_attribution"
            matched_dir: pathlib.Path = pathlib.Path(args.matched_dir)

            # National + parquet output → stream each RHS chunk directly to file
            streaming: bool = bbox is None and args.output == "parquet"
            output_path: pathlib.Path | None = (
                matched_dir / f"{stem}.parquet" if streaming else None
            )

            table: pa.Table = matcher.match_dispatch(
                mode=args.mode,
                lhs=args.lhs_name,
                bbox=bbox,
                explain=args.explain,
                n_chunks=args.batches,
                distance_m=args.distance,
                phase3_distance_m=args.phase3_distance,
                rhs_id_col=args.rhs_id_col,
                output_path=output_path,
                overlap_threshold=args.overlap_threshold,
                phase4_tolerance_m=args.phase4_tolerance,
                usrn_line_parquet=(
                    pathlib.Path(args.usrn_line_parquet)
                    if args.usrn_line_parquet is not None
                    else None
                ),
            )
            if streaming:
                log.info("Streaming output written to %s", output_path)
            else:
                matcher.output_writer(
                    table,
                    output=args.output,
                    matched_dir=matched_dir,
                    stem=stem,
                    sample=args.sample_rows,
                )


def _validate_input_file(path: pathlib.Path) -> None:
    """Check the input dir exists, all filenames are lowercase, and the target file is present."""
    if not DEFAULT_INPUT_DIR.exists():
        raise ValueError(
            f"Input directory '{DEFAULT_INPUT_DIR}' does not exist. Run 'geo-matcher init' first."
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
    print("Initialising geo-matcher project directories:")  # noqa: T201
    for d in dirs:
        created: bool = not d.exists()
        d.mkdir(exist_ok=True)
        status: str = "created" if created else "already exists"
        print(f"  {d}/  ({status})")  # noqa: T201
