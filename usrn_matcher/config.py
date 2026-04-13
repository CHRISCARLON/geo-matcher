import pathlib
import re


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
        Path to the source file (GeoPackage, Shapefile, or anything geopandas
        can read with pyogrio).
    parquet_path:
        Where the prepared GeoParquet file is written/cached. Defaults to
        ``output_data/{name}_27700.parquet``.
    columns:
        Columns to SELECT from this dataset in the spatial join. An empty list
        means all columns (excluding ``geometry`` and the internal ``bbox``
        covering column) are selected automatically.
    geometry_column:
        Name of the geometry column in the source file. Set to ``"SHAPE"`` for
        datasets that use a non-standard name. The column is renamed to
        ``"geometry"`` during preparation.
    row_group_size:
        Row group size when writing GeoParquet. Smaller values give finer
        spatial pruning but a larger file footer. ``10_000`` suits polygon
        datasets; ``20_000`` suits line datasets with many rows.
    crs:
        Expected CRS as an EPSG string. This is an assertion — reprojection is
        NOT performed. Defaults to ``"EPSG:27700"`` (British National Grid).
    """

    name: str
    source_path: pathlib.Path
    parquet_path: pathlib.Path
    columns: list[str]
    geometry_column: str
    row_group_size: int
    crs: str

    def __init__(
        self,
        name: str,
        source_path: str | pathlib.Path,
        parquet_path: str | pathlib.Path | None = None,
        columns: list[str] | None = None,
        geometry_column: str = "geometry",
        row_group_size: int = 10_000,
        crs: str = "EPSG:27700",
    ) -> None:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            raise ValueError(
                f"DatasetConfig.name {name!r} must be a valid SQL identifier "
                "(letters, digits, underscores; must not start with a digit)."
            )
        self.name = name
        self.source_path = pathlib.Path(source_path)
        self.parquet_path = (
            pathlib.Path(parquet_path)
            if parquet_path is not None
            else pathlib.Path(f"output_data/{name}_27700.parquet")
        )
        self.columns = columns if columns is not None else []
        self.geometry_column = geometry_column
        self.row_group_size = row_group_size
        self.crs = crs

    def __repr__(self) -> str:
        return (
            f"DatasetConfig("
            f"name={self.name!r}, "
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
            and self.source_path == other.source_path
            and self.parquet_path == other.parquet_path
            and self.columns == other.columns
            and self.geometry_column == other.geometry_column
            and self.row_group_size == other.row_group_size
            and self.crs == other.crs
        )
