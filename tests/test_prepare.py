"""Tests for the pre-spatial phase: _write_geoparquet and prepare_dataset."""

import json

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

from usrn_matcher.config import DatasetConfig
from usrn_matcher.prepare import _write_geoparquet, prepare_dataset

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def tiny_gdf():
    """10-row synthetic GeoDataFrame — no file I/O required."""
    geoms = [box(i * 1000, i * 1000, i * 1000 + 500, i * 1000 + 500) for i in range(10)]
    return gpd.GeoDataFrame(
        {"val": range(10), "category": ["a", "b"] * 5, "geometry": geoms},
        crs="EPSG:27700",
    )


@pytest.fixture(scope="module")
def written_parquet(tiny_gdf, tmp_path_factory):
    """GeoParquet written from the synthetic GDF."""
    out = tmp_path_factory.mktemp("parquet") / "test.parquet"
    _write_geoparquet(tiny_gdf, out, row_group_size=5)
    return out


# ---------------------------------------------------------------------------
# _write_geoparquet metadata tests
# ---------------------------------------------------------------------------


def test_geoparquet_version(written_parquet):
    geo = json.loads(pq.read_schema(written_parquet).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_covering_metadata(written_parquet):
    geo = json.loads(pq.read_schema(written_parquet).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_compression_zstd(written_parquet):
    rg = pq.ParquetFile(written_parquet).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        assert col.compression == "ZSTD", (
            f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
        )


def test_row_group_size(written_parquet, tiny_gdf):
    """Row group size is respected."""
    meta = pq.ParquetFile(written_parquet).metadata
    assert meta.num_row_groups == len(tiny_gdf) // 5


# ---------------------------------------------------------------------------
# prepare_dataset tests
# ---------------------------------------------------------------------------


def test_prepare_dataset_skips_when_exists(tmp_path):
    """prepare_dataset returns immediately (no file read) when output exists and force=False."""
    out = tmp_path / "fake_27700.parquet"
    out.touch()  # simulate pre-existing file

    cfg = DatasetConfig(
        name="fake",
        source_path=tmp_path / "does_not_exist.gpkg",  # would error if read
        parquet_path=out,
    )
    result = prepare_dataset(cfg, force=False)
    assert result == out
    # File unchanged (still empty touch)
    assert out.stat().st_size == 0


def test_prepare_dataset_force_overwrites(tiny_gdf, tmp_path, monkeypatch):
    """prepare_dataset re-writes the file when force=True."""
    import geopandas as _gpd

    out = tmp_path / "forced_27700.parquet"
    out.touch()

    cfg = DatasetConfig(
        name="forced",
        source_path=tmp_path / "src.gpkg",
        parquet_path=out,
    )

    # Monkeypatch gpd.read_file to return our synthetic GDF
    monkeypatch.setattr(_gpd, "read_file", lambda *a, **kw: tiny_gdf)

    result = prepare_dataset(cfg, force=True)
    assert result == out
    assert out.stat().st_size > 0  # actual parquet content written


def test_prepare_dataset_geometry_rename(tiny_gdf, tmp_path, monkeypatch):
    """geometry_column != 'geometry' triggers rename before writing."""
    import geopandas as _gpd

    # Build a GDF with a non-standard geometry column name
    alt_gdf = tiny_gdf.rename_geometry("SHAPE")

    out = tmp_path / "renamed_27700.parquet"
    cfg = DatasetConfig(
        name="renamed",
        source_path=tmp_path / "src.gpkg",
        parquet_path=out,
        geometry_column="SHAPE",
    )

    monkeypatch.setattr(_gpd, "read_file", lambda *a, **kw: alt_gdf)

    result = prepare_dataset(cfg, force=True)
    # Parquet should have a "geometry" column, not "SHAPE"
    schema = pq.read_schema(str(result))
    col_names = schema.names
    assert "geometry" in col_names
    assert "SHAPE" not in col_names


def test_prepare_dataset_wrong_crs_raises(tiny_gdf, tmp_path, monkeypatch):
    """AssertionError raised when the source CRS doesn't match config.crs."""
    import geopandas as _gpd

    wrong_crs_gdf = tiny_gdf.to_crs("EPSG:4326")
    out = tmp_path / "wrong_crs.parquet"
    cfg = DatasetConfig(
        name="bad",
        source_path=tmp_path / "src.gpkg",
        parquet_path=out,
        crs="EPSG:27700",
    )

    monkeypatch.setattr(_gpd, "read_file", lambda *a, **kw: wrong_crs_gdf)

    with pytest.raises(AssertionError, match="EPSG:27700"):
        prepare_dataset(cfg, force=True)
