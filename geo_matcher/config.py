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
    """Which base dataset a join runs from — keys the second axis of the join registry.

    USRN joins (street centrelines) and UPRN joins (address points) can both
    register a strategy for the same RHS ``GeometryType`` (e.g. both have a
    ``polygon`` join) without colliding, because the registry is keyed by
    ``(LhsKind, GeometryType)`` rather than ``GeometryType`` alone.
    """

    USRN = "usrn"
    UPRN = "uprn"


@dataclass(frozen=True)
class OgrSource:
    """Any GDAL-readable vector format (GeoPackage, Shapefile, etc.).

    If a transform into/out of EPSG:27700 is needed, set ``nadgrids_path`` to the
    ``.gsb`` binary grid extracted from the OS OSTN15 "NTv2 format files" ZIP
    (https://www.ordnancesurvey.co.uk/products/os-net/for-developers — not the
    ZIP itself, the ``.gsb`` file inside it, e.g. ``OSTN15_NTv2_OSGBtoETRS.gsb``)
    for full accuracy; otherwise DuckDB falls back to its own default
    (grid-less) coordinate operation search.

    ``target_crs`` is the CRS this source is prepared into (EPSG:27700 by
    default). The prepare step always detects the file's actual CRS itself
    (from the OGR source's own metadata) and transforms from that detected
    CRS to ``target_crs`` when they differ. ``source_crs`` is optional — what
    you believe the file is in; if it disagrees with what's detected, prepare
    logs a warning and uses the detected CRS anyway (the file wins).
    """

    path: pathlib.Path
    source_crs: str | None = None
    target_crs: str = "EPSG:27700"
    row_group_size: int = 20_000
    nadgrids_path: pathlib.Path | None = None


@dataclass(frozen=True)
class CsvSource:
    """CSV file with explicit x/y coordinate columns, or WKT text for line/polygon geometries.

    If the coordinate/WKT values are in a different CRS than ``target_crs`` (the
    target, EPSG:27700 by default), set ``source_crs`` to that CRS; the prepare
    step will transform to ``target_crs``. If that transform is into/out of
    EPSG:27700, also set ``nadgrids_path`` to the OS OSTN15 ``.gsb`` grid for
    full accuracy — see ``OgrSource`` for where to get it.

    Unlike ``OgrSource``/``ParquetSource``, a CSV carries no CRS metadata to
    read. Prepare instead makes a best-effort guess from the coordinates
    themselves: values within Great Britain's WGS84 lon/lat extent
    are detected as ``EPSG:4326``, values within the British National Grid's
    valid eastings/northings as ``EPSG:27700``. When detection succeeds it's
    used for the transform, exactly like ``OgrSource``/``ParquetSource`` — and
    if it disagrees with a declared ``source_crs``, prepare logs a warning and
    uses the detected CRS anyway. When the extent doesn't clearly fall in
    either range, detection is skipped and ``source_crs`` is used as declared,
    with no warning.
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

    For files produced by this pipeline the geometry column is always named
    ``"geometry"`` and stored as WKB — the defaults handle that automatically.

    For external Parquet files where the geometry column has a different name or
    is stored as a native GEOMETRY type (e.g. ``GEOMETRY('OGC:CRS84')``), set
    ``geometry_col`` to the source column name; the prepare step will transform
    to ``target_crs`` (EPSG:27700) if needed. If that transform is into/out of
    EPSG:27700, also set ``nadgrids_path`` to the OS OSTN15 ``.gsb`` grid for
    full accuracy — see ``OgrSource`` for where to get it.

    Prepare always auto-detects the CRS from the file's own GeoParquet ``geo``
    metadata (mirroring ``OgrSource``'s auto-detection from the source file),
    and raises if that metadata is absent — regardless of whether
    ``source_crs`` is set. ``source_crs`` is optional — what you believe the
    file is in; if it disagrees with what's detected, prepare logs a warning
    and uses the detected CRS anyway (the file wins).
    """

    path: pathlib.Path
    source_crs: str | None = None
    target_crs: str = "EPSG:27700"
    row_group_size: int = 20_000
    geometry_col: str = "geometry"
    nadgrids_path: pathlib.Path | None = None


@dataclass(frozen=True)
class UsrnSource:
    """USRN preparation, in one of two modes selected by ``buffer_m``.

    ``buffer_m=None`` (default) — ``path`` points at a raw OGR-readable USRN
    source (e.g. the OS Open USRN GeoPackage). Produces a plain Hilbert-sorted
    centreline GeoParquet — equivalent to preparing ``path`` via ``OgrSource``.

    ``buffer_m=<float>`` — ``path`` points at an already-prepared USRN
    centreline GeoParquet (e.g. the output of the ``buffer_m=None`` mode).
    Produces a buffered corridor GeoParquet for line-join Phase 2, where
    ``geometry`` is ``ST_Buffer(centreline, buffer_m)`` (the join predicate)
    and ``geometry_line`` is the original centreline WKB (used for distance
    and overlap calculations). ``buffer_m`` must be >= ``--distance`` at
    match time.

    When ``buffer_m is None`` and the raw source needs a transform into
    EPSG:27700, set ``nadgrids_path`` to the OS OSTN15 ``.gsb`` grid for full
    accuracy — see ``OgrSource`` for where to get it. Forwarded to the
    transient ``OgrSource`` this delegates to; unused in buffered mode (no
    transform happens there).
    """

    path: pathlib.Path
    crs: str = "EPSG:27700"
    buffer_m: float | None = None
    row_group_size: int = 20_000
    nadgrids_path: pathlib.Path | None = None


@dataclass(frozen=True)
class UprnSource:
    """UPRN preparation, in one of two modes selected by ``buffer_m``.

    ``buffer_m=None`` (default) — ``path`` points at a raw OGR-readable UPRN
    source (e.g. the OS Open UPRN GeoPackage). Produces a plain Hilbert-sorted
    address-point GeoParquet with just ``uprn`` and ``geometry`` — the
    source's uppercase ``UPRN`` id is renamed to match this pipeline's
    lowercase convention, and ``X_COORDINATE``/``Y_COORDINATE``/``LATITUDE``/
    ``LONGITUDE`` are dropped as redundant with ``geometry`` (same point, two
    encodings) — at 40M+ rows that's four fewer doubles per row.

    ``buffer_m=<float>`` — ``path`` points at an already-prepared UPRN
    GeoParquet (e.g. the output of the ``buffer_m=None`` mode). Produces a
    buffered catchment-polygon GeoParquet, where ``geometry`` is
    ``ST_Buffer(point, buffer_m)`` (the join predicate) and ``geometry_point``
    is the original point WKB, alongside ``uprn``.

    When ``buffer_m is None`` and the raw source needs a transform into
    EPSG:27700, set ``nadgrids_path`` to the OS OSTN15 ``.gsb`` grid for full
    accuracy — see ``OgrSource`` for where to get it. Forwarded to the
    transient ``OgrSource`` this delegates to; unused in buffered mode (no
    transform happens there).
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
        Typed source descriptor (``OgrSource``, ``CsvSource``, ``ParquetSource``,
        or ``UsrnSource``). When provided, ``source_path`` is derived from
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
