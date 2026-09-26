import pathlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias

BBox: TypeAlias = Sequence[float]


class GeometryType(StrEnum):
    POINT = "point"
    LINE = "line"
    POLYGON = "polygon"


class LhsKind(StrEnum):
    """Base dataset a join runs from. Keyed with ``GeometryType`` in the join
    registry so ``usrn`` and ``uprn`` can each register a ``polygon`` join
    without colliding.
    """

    USRN = "usrn"
    UPRN = "uprn"


@dataclass(frozen=True)
class OgrSource:
    """Any GDAL-readable vector format (GeoPackage, Shapefile, etc.).

    Source CRS is always auto-detected from the file's own metadata; a
    mismatched ``source_crs`` only logs a warning (the detected CRS wins).
    Set ``nadgrids_path`` to the OSTN15 NTv2 ``.gsb`` grid for accurate
    EPSG:27700 transforms — download from
    https://www.ordnancesurvey.co.uk/products/os-net/for-developers.
    """

    path: pathlib.Path
    source_crs: str | None = None
    target_crs: str = "EPSG:27700"
    row_group_size: int = 20_000
    nadgrids_path: pathlib.Path | None = None


@dataclass(frozen=True)
class CsvSource:
    """CSV with x/y coordinate columns (points), or a WKT column (lines/polygons).

    Unlike ``OgrSource``/``ParquetSource``, a CSV carries no CRS metadata:
    prepare guesses from the coordinate extent (GB lon/lat → EPSG:4326, BNG
    eastings/northings → EPSG:27700) and warns if that disagrees with a
    declared ``source_crs``. If the extent is ambiguous, ``source_crs`` is
    used as declared, with no warning. See ``OgrSource`` for ``nadgrids_path``.
    """

    path: pathlib.Path
    x_col: str = "Easting"
    y_col: str = "Northing"
    geometry_type: GeometryType | str = GeometryType.POINT
    wkt_col: str | None = None
    row_group_size: int = 20_000
    target_crs: str = "EPSG:27700"
    source_crs: str | None = None
    nadgrids_path: pathlib.Path | None = None

    def __post_init__(self) -> None:
        geometry_type: GeometryType = GeometryType(self.geometry_type)
        object.__setattr__(self, "geometry_type", geometry_type)

        if (
            geometry_type in (GeometryType.LINE, GeometryType.POLYGON)
            and self.wkt_col is None
        ):
            raise ValueError(
                f"CsvSource.wkt_col is required when geometry_type={geometry_type.value!r} "
                "— LINE/POLYGON geometries are built from a WKT text column "
                "(POINT is the only geometry_type built from x_col/y_col)."
            )


@dataclass(frozen=True)
class ParquetSource:
    """Existing GeoParquet to re-sort and re-compress.

    CRS is always auto-detected from the file's GeoParquet ``geo`` metadata
    (raises if absent); a mismatched ``source_crs`` only logs a warning.
    Set ``geometry_col`` for a non-standard column name or a native GEOMETRY
    type (e.g. ``GEOMETRY('OGC:CRS84')``). See ``OgrSource`` for ``nadgrids_path``.
    """

    path: pathlib.Path
    source_crs: str | None = None
    target_crs: str = "EPSG:27700"
    row_group_size: int = 20_000
    geometry_col: str = "geometry"
    nadgrids_path: pathlib.Path | None = None


@dataclass(frozen=True)
class UsrnSource:
    """USRN preparation — mode selected by ``buffer_m``.

    - ``buffer_m=None`` (default): ``path`` is a raw OGR source; produces a
      plain centreline GeoParquet, like ``OgrSource``.
    - ``buffer_m=<float>``: ``path`` is an already-prepared centreline
      GeoParquet; produces a buffered corridor for line-join Phase 2
      (``geometry`` = buffered polygon, ``geometry_line`` = original
      centreline). Must be >= ``--distance`` at match time.

    ``nadgrids_path`` only applies in plain mode — see ``OgrSource``.
    """

    path: pathlib.Path
    crs: str = "EPSG:27700"
    buffer_m: float | None = None
    row_group_size: int = 20_000
    nadgrids_path: pathlib.Path | None = None


@dataclass(frozen=True)
class UprnSource:
    """UPRN preparation — mode selected by ``buffer_m``.

    - ``buffer_m=None`` (default): ``path`` is a raw OGR source; produces a
      plain address-point GeoParquet with just ``uprn`` + ``geometry`` —
      the source's uppercase ``UPRN`` is renamed lowercase, and
      ``X/Y_COORDINATE``/``LATITUDE``/``LONGITUDE`` dropped as redundant.
    - ``buffer_m=<float>``: ``path`` is an already-prepared point GeoParquet;
      produces buffered catchment polygons (``geometry`` = buffered polygon,
      ``geometry_point`` = original point).

    ``nadgrids_path`` only applies in plain mode — see ``OgrSource``.
    """

    path: pathlib.Path
    crs: str = "EPSG:27700"
    buffer_m: float | None = None
    row_group_size: int = 20_000
    nadgrids_path: pathlib.Path | None = None


MatchSource: TypeAlias = OgrSource | CsvSource | ParquetSource
"""The three formats usable as the RHS/match dataset."""

AnySource: TypeAlias = MatchSource | UsrnSource | UprnSource

DEFAULT_INPUT_DIR: pathlib.Path = pathlib.Path("input_data")
DEFAULT_OUTPUT_DIR: pathlib.Path = pathlib.Path("output_data")
DEFAULT_MATCHED_DIR: pathlib.Path = pathlib.Path("matched_data")
DEFAULT_USRN_GPKG: pathlib.Path = DEFAULT_INPUT_DIR / "osopenusrn.gpkg"
DEFAULT_UPRN_GPKG: pathlib.Path = DEFAULT_INPUT_DIR / "osopenuprn.gpkg"


class DatasetConfig:
    """Describes a spatial dataset for use as the right-hand side of a USRN join.

    Provide either ``source`` (typed descriptor — preferred, drives which
    reader ``prepare()`` dispatches to) or ``source_path``. ``name`` must be
    a valid SQL identifier. ``columns`` empty means every column except
    ``geometry``/``bbox`` is auto-selected. ``geometry_column``,
    ``row_group_size`` and ``crs`` are kept for backward compatibility —
    prefer setting them on ``source`` instead.
    """

    name: str
    source_path: pathlib.Path
    source: AnySource | None
    parquet_path: pathlib.Path
    columns: list[str]
    geometry_column: str
    row_group_size: int
    crs: str

    def __init__(
        self,
        name: str,
        source_path: str | pathlib.Path | None = None,
        parquet_path: str | pathlib.Path | None = None,
        columns: list[str] | None = None,
        geometry_column: str = "geometry",
        row_group_size: int = 10_000,
        crs: str = "EPSG:27700",
        source: AnySource | None = None,
    ) -> None:
        if source_path is None and source is None:
            raise ValueError("Provide either source_path or source.")
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            raise ValueError(
                f"DatasetConfig.name {name!r} must be a valid SQL identifier "
                "(letters, digits, underscores; must not start with a digit)."
            )
        self.source = source
        self.source_path = pathlib.Path(
            source_path if source_path is not None else source.path  # type: ignore[union-attr]
        )
        self.name = name
        self.parquet_path = (
            pathlib.Path(parquet_path)
            if parquet_path is not None
            else DEFAULT_OUTPUT_DIR / f"{name}_27700.parquet"
        )
        self.columns = columns if columns is not None else []
        self.geometry_column = geometry_column
        self.row_group_size = row_group_size
        self.crs = crs

    def __repr__(self) -> str:
        return (
            f"DatasetConfig("
            f"name={self.name!r}, "
            f"source={self.source!r}, "
            f"source_path={self.source_path!r}, "
            f"parquet_path={self.parquet_path!r}, "
            f"columns={self.columns!r}, "
            f"geometry_column={self.geometry_column!r}, "
            f"row_group_size={self.row_group_size!r}, "
            f"crs={self.crs!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DatasetConfig):
            return NotImplemented
        return (
            self.name == other.name
            and self.source == other.source
            and self.source_path == other.source_path
            and self.parquet_path == other.parquet_path
            and self.columns == other.columns
            and self.geometry_column == other.geometry_column
            and self.row_group_size == other.row_group_size
            and self.crs == other.crs
        )
