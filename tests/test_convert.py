"""Tests for _write_geoparquet()."""

import json

import geopandas as gpd
import pyarrow.parquet as pq
import pytest

from usrn_soil_matcher.constants import SOIL_GPKG
from usrn_soil_matcher.convert import _write_geoparquet


@pytest.fixture(scope="module")
def written_parquet(tmp_path_factory):
    gdf = gpd.read_file(SOIL_GPKG, engine="pyogrio", rows=2048)
    out = tmp_path_factory.mktemp("parquet") / "test.parquet"
    _write_geoparquet(gdf, out, row_group_size=512)
    return out


def test_geoparquet_version(written_parquet):
    """
    Does the metadata patching work?

    TODO: Need to change this at some point - famous last words.
    """
    geo = json.loads(pq.read_schema(written_parquet).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_covering_metadata(written_parquet):
    """
    Check that the covering metadata is there for Sedona to use.
    """
    geo = json.loads(pq.read_schema(written_parquet).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_compression_zstd(written_parquet):
    """All columns must use ZSTD compression."""
    rg = pq.ParquetFile(written_parquet).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        assert col.compression == "ZSTD", (
            f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
        )


def test_string_columns_rle_dictionary(written_parquet):
    """Low-cardinality string columns use RLE_DICTIONARY encoding."""
    rg = pq.ParquetFile(written_parquet).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        print(f"\n{col.path_in_schema}: {col.compression} {col.encodings}")
        if col.path_in_schema in {"MAP_SYMBOL", "DRAINAGE", "SOILSCAPE", "FERTILITY"}:
            assert "RLE_DICTIONARY" in col.encodings, (
                f"{col.path_in_schema}: expected RLE_DICTIONARY, got {col.encodings}"
            )
