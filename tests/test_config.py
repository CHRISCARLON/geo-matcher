"""Tests for DatasetConfig."""

import pathlib

import pytest

from usrn_matcher.config import DatasetConfig

pytestmark = pytest.mark.unit


def test_parquet_path_defaults_to_name():
    cfg = DatasetConfig(name="flood_risk", source_path="input_data/flood.gpkg")
    assert cfg.parquet_path == pathlib.Path("output_data/flood_risk_27700.parquet")


def test_invalid_name_raises():
    with pytest.raises(ValueError, match="valid SQL identifier"):
        DatasetConfig(name="my-dataset", source_path="x.gpkg")

    with pytest.raises(ValueError, match="valid SQL identifier"):
        DatasetConfig(name="1dataset", source_path="x.gpkg")


def test_columns_default_empty():
    cfg = DatasetConfig(name="x", source_path="x.gpkg")
    assert cfg.columns == []
