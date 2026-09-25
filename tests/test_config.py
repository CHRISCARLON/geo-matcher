"""Unit tests for DatasetConfig."""

import pathlib

import pytest

from geo_matcher.config import (
    CsvSource,
    DatasetConfig,
    GeometryType,
    UsrnSource,
)

pytestmark = pytest.mark.unit


def test_parquet_path_defaults_to_name():
    """parquet_path defaults to output_data/<name>_27700.parquet."""
    cfg = DatasetConfig(name="flood_risk", source_path="input_data/flood.gpkg")
    assert cfg.parquet_path == pathlib.Path("output_data/flood_risk_27700.parquet")


def test_invalid_name_raises():
    """Names that aren't valid SQL identifiers are rejected."""
    with pytest.raises(ValueError, match="valid SQL identifier"):
        DatasetConfig(name="my-dataset", source_path="x.gpkg")

    with pytest.raises(ValueError, match="valid SQL identifier"):
        DatasetConfig(name="1dataset", source_path="x.gpkg")


@pytest.mark.parametrize("value", ["point", "line", "polygon"])
def test_csv_source_geometry_type_coerces_string(value):
    """A plain string is normalised to the matching GeometryType member."""
    wkt_col = "wkt" if value in ("line", "polygon") else None
    src = CsvSource(path=pathlib.Path("a.csv"), geometry_type=value, wkt_col=wkt_col)
    assert isinstance(src.geometry_type, GeometryType)
    assert src.geometry_type == value


@pytest.mark.parametrize("geometry_type", ["line", "polygon"])
def test_csv_source_wkt_col_required_for_line_and_polygon(geometry_type):
    """LINE/POLYGON geometry_type without wkt_col is rejected at construction time."""
    with pytest.raises(ValueError, match="wkt_col"):
        CsvSource(path=pathlib.Path("a.csv"), geometry_type=geometry_type)


def test_usrn_source_crs_defaults_to_epsg_27700():
    """crs defaults to 'EPSG:27700', matching every other source struct."""
    src = UsrnSource(path=pathlib.Path("osopenusrn.gpkg"))
    assert src.crs == "EPSG:27700"
