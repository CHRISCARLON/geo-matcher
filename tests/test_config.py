"""Tests for DatasetConfig."""

import pathlib

import pytest

from usrn_matcher.config import CsvSource, DatasetConfig, GeometryType

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


def test_columns_default_empty():
    """columns defaults to an empty list."""
    cfg = DatasetConfig(name="x", source_path="x.gpkg")
    assert cfg.columns == []


# ---------------------------------------------------------------------------
# CsvSource.geometry_type
# ---------------------------------------------------------------------------


def test_csv_source_geometry_type_defaults_to_point():
    """The default geometry_type is the GeometryType.POINT member."""
    src = CsvSource(path=pathlib.Path("a.csv"))
    assert src.geometry_type is GeometryType.POINT


@pytest.mark.parametrize("value", ["point", "line", "polygon"])
def test_csv_source_geometry_type_coerces_string(value):
    """A plain string is normalised to the matching GeometryType member."""
    kwargs = {"wkt_col": "wkt"} if value in ("line", "polygon") else {}
    src = CsvSource(path=pathlib.Path("a.csv"), geometry_type=value, **kwargs)
    assert isinstance(src.geometry_type, GeometryType)
    assert src.geometry_type == value


def test_csv_source_rejects_unknown_geometry_type():
    """An unknown geometry_type is rejected at construction time."""
    with pytest.raises(ValueError, match="not a valid GeometryType"):
        CsvSource(path=pathlib.Path("a.csv"), geometry_type="hexagon")


# ---------------------------------------------------------------------------
# CsvSource.wkt_col
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("geometry_type", ["line", "polygon"])
def test_csv_source_wkt_col_required_for_line_and_polygon(geometry_type):
    """LINE/POLYGON geometry_type without wkt_col is rejected at construction time."""
    with pytest.raises(ValueError, match="wkt_col"):
        CsvSource(path=pathlib.Path("a.csv"), geometry_type=geometry_type)


def test_csv_source_wkt_col_not_required_for_point():
    """POINT (the default) does not require wkt_col."""
    src = CsvSource(path=pathlib.Path("a.csv"), geometry_type="point")
    assert src.wkt_col is None


@pytest.mark.parametrize("geometry_type", ["line", "polygon"])
def test_csv_source_wkt_col_accepted_for_line_and_polygon(geometry_type):
    """wkt_col is stored as given when geometry_type is line/polygon."""
    src = CsvSource(
        path=pathlib.Path("a.csv"), geometry_type=geometry_type, wkt_col="wkt"
    )
    assert src.wkt_col == "wkt"
