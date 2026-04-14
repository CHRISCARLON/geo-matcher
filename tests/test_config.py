"""Tests for DatasetConfig."""

import pathlib

import pytest

from usrn_matcher.config import DatasetConfig

pytestmark = pytest.mark.unit


def test_parquet_path_defaults_to_name():
    cfg = DatasetConfig(name="flood_risk", source_path="input_data/flood.gpkg")
    assert cfg.parquet_path == pathlib.Path("output_data/flood_risk_27700.parquet")


def test_explicit_parquet_path_respected():
    cfg = DatasetConfig(
        name="soil",
        source_path="input_data/soil.gpkg",
        parquet_path="custom/output.parquet",
    )
    assert cfg.parquet_path == pathlib.Path("custom/output.parquet")


def test_source_path_normalised_to_pathlib():
    cfg = DatasetConfig(name="x", source_path="a/b/c.gpkg")
    assert isinstance(cfg.source_path, pathlib.Path)


def test_parquet_path_normalised_to_pathlib():
    cfg = DatasetConfig(name="x", source_path="a.gpkg", parquet_path="b.parquet")
    assert isinstance(cfg.parquet_path, pathlib.Path)


def test_invalid_name_hyphen_raises():
    with pytest.raises(ValueError, match="valid SQL identifier"):
        DatasetConfig(name="my-dataset", source_path="x.gpkg")


def test_invalid_name_starts_with_digit_raises():
    with pytest.raises(ValueError, match="valid SQL identifier"):
        DatasetConfig(name="1dataset", source_path="x.gpkg")


def test_valid_name_with_underscore():
    cfg = DatasetConfig(name="flood_risk_2024", source_path="x.gpkg")
    assert cfg.name == "flood_risk_2024"


def test_columns_default_empty():
    cfg = DatasetConfig(name="x", source_path="x.gpkg")
    assert cfg.columns == []


def test_columns_stored():
    cfg = DatasetConfig(name="x", source_path="x.gpkg", columns=["a", "b"])
    assert cfg.columns == ["a", "b"]


def test_defaults():
    cfg = DatasetConfig(name="x", source_path="x.gpkg")
    assert cfg.geometry_column == "geometry"
    assert cfg.row_group_size == 10_000
    assert cfg.crs == "EPSG:27700"
