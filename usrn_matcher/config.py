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


@dataclass(frozen=True)
class OgrSource:
    """Any GDAL-readable vector format (GeoPackage, Shapefile, etc.)."""

    path: pathlib.Path
    crs: str = "EPSG:27700"
    row_group_size: int = 20_000


@dataclass(frozen=True)
class CsvSource:
    """CSV file with explicit x/y coordinate columns."""

    path: pathlib.Path
    x_col: str = "Easting"
    y_col: str = "Northing"
    geometry_type: str = GeometryType.POINT
    crs: str = "EPSG:27700"
    row_group_size: int = 20_000


@dataclass(frozen=True)
class ParquetSource:
    """Existing GeoParquet (or any geometry-bearing Parquet) to re-sort and re-compress.

    For files produced by this pipeline the geometry column is always named
    ``"geometry"`` and stored as WKB — the defaults handle that automatically.

    For external Parquet files where the geometry column has a different name or
    is stored as a native GEOMETRY type (e.g. ``GEOMETRY('OGC:CRS84')``), set
    ``geometry_col`` to the source column name and ``source_crs`` to the CRS of
    that column; the prepare step will transform to ``crs`` (EPSG:27700).
    """

    path: pathlib.Path
    crs: str = "EPSG:27700"
    row_group_size: int = 20_000
    geometry_col: str = "geometry"
    source_crs: str | None = None


@dataclass(frozen=True)
class UsrnLineSource:
    """Buffered USRN centreline GeoParquet for line join Phase 2.

    Reads an existing ``usrns_27700.parquet`` and produces a new file where
    ``geometry`` is ``ST_Buffer(centreline, buffer_m)`` (the join predicate) and
    ``geometry_line`` is the original centreline WKB (used for distance and
    overlap calculations). ``buffer_m`` must be >= ``--distance`` at match time.
    """

    path: pathlib.Path
    buffer_m: float
    row_group_size: int = 20_000


AnySource: TypeAlias = OgrSource | CsvSource | ParquetSource | UsrnLineSource

DEFAULT_INPUT_DIR: pathlib.Path = pathlib.Path("input_data")
DEFAULT_OUTPUT_DIR: pathlib.Path = pathlib.Path("output_data")
DEFAULT_MATCHED_DIR: pathlib.Path = pathlib.Path("matched_data")
DEFAULT_USRN_GPKG: pathlib.Path = DEFAULT_INPUT_DIR / "osopenusrn.gpkg"


class DatasetConfig:
    """Describes a spatial dataset for use as the right-hand side of a USRN join.

    Parameters
    ----------
    name:
        Short identifier used in output filenames, SQL view names, and log
        messages. Must be a valid SQL identifier (letters, digits, underscores;
        must not start with a digit). E.g. ``"soil"``, ``"highways"``,
        ``"flood_risk"``.
    source_path:
        Path to the source file. Mutually optional with ``source`` — provide
        one or the other. Kept for backward compatibility; prefer ``source``.
    source:
        Typed source descriptor (``OgrSource``, ``CsvSource``, or
        ``ParquetSource``). When provided, ``source_path`` is derived from
        ``source.path`` if not given explicitly. The ``prepare()`` function
        dispatches on this type to choose the correct reader.
    parquet_path:
        Where the prepared GeoParquet file is written/cached. Defaults to
        ``output_data/{name}_27700.parquet``.
    columns:
        Columns to SELECT from this dataset in the spatial join. An empty list
        means all columns (excluding ``geometry`` and the internal ``bbox``
        covering column) are selected automatically.
    geometry_column:
        Name of the geometry column in the source file. Kept for backward
        compatibility — the prepare pipeline auto-detects this via DuckDB.
    row_group_size:
        Row group size when writing GeoParquet. Kept for backward compatibility
        — prefer setting this on the ``source`` struct instead.
    crs:
        Expected CRS as an EPSG string. This is an assertion — reprojection is
        NOT performed. Kept for backward compatibility — prefer setting this on
        the ``source`` struct. Defaults to ``"EPSG:27700"`` (British National Grid).
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
