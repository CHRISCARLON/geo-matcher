"""Tests for the pre-spatial phase: prepare_dataset and prepare_from_csv."""

import csv
import json

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

from usrn_matcher.config import DatasetConfig
from usrn_matcher.prepare import prepare_dataset, prepare_from_csv

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


@pytest.fixture
def tiny_gpkg(tiny_gdf, tmp_path):
    """Write tiny_gdf to a real GeoPackage for DuckDB-based prepare tests."""
    p = tmp_path / "tiny.gpkg"
    tiny_gdf.to_file(str(p), driver="GPKG")
    return p


@pytest.fixture
def prepared_parquet(tiny_gpkg, tmp_path):
    """GeoParquet written from the synthetic GDF via prepare_dataset."""
    out = tmp_path / "test.parquet"
    cfg = DatasetConfig(name="test", source_path=tiny_gpkg, parquet_path=out)
    prepare_dataset(cfg, force=True)
    return out


# ---------------------------------------------------------------------------
# prepare_dataset metadata tests
# ---------------------------------------------------------------------------


def test_geoparquet_version(prepared_parquet):
    geo = json.loads(pq.read_schema(prepared_parquet).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_covering_metadata(prepared_parquet):
    geo = json.loads(pq.read_schema(prepared_parquet).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_compression_zstd(prepared_parquet):
    rg = pq.ParquetFile(prepared_parquet).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        assert col.compression == "ZSTD", (
            f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
        )


def test_crs_in_metadata(prepared_parquet):
    geo = json.loads(pq.read_schema(prepared_parquet).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None, "CRS should be present in geometry column metadata"
    assert "27700" in str(crs_meta)


# ---------------------------------------------------------------------------
# prepare_dataset behaviour tests
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


def test_prepare_dataset_force_overwrites(tiny_gpkg, tmp_path):
    """prepare_dataset re-writes the file when force=True."""
    out = tmp_path / "forced_27700.parquet"
    out.touch()

    cfg = DatasetConfig(
        name="forced",
        source_path=tiny_gpkg,
        parquet_path=out,
    )

    result = prepare_dataset(cfg, force=True)
    assert result == out
    assert out.stat().st_size > 0  # actual parquet content written


def test_prepare_dataset_geometry_renamed(tiny_gpkg, tmp_path):
    """DuckDB always outputs the geometry column as 'geometry'."""
    out = tmp_path / "renamed_27700.parquet"
    cfg = DatasetConfig(
        name="renamed",
        source_path=tiny_gpkg,
        parquet_path=out,
    )

    result = prepare_dataset(cfg, force=True)
    schema = pq.read_schema(str(result))
    assert "geometry" in schema.names
    assert "geom" not in schema.names


def test_prepare_dataset_wrong_crs_raises(tiny_gdf, tmp_path):
    """AssertionError raised when the source CRS doesn't match config.crs."""
    wrong_crs_gdf = tiny_gdf.to_crs("EPSG:4326")
    src_gpkg = tmp_path / "wrong_crs.gpkg"
    wrong_crs_gdf.to_file(str(src_gpkg), driver="GPKG")

    out = tmp_path / "wrong_crs.parquet"
    cfg = DatasetConfig(
        name="bad",
        source_path=src_gpkg,
        parquet_path=out,
        crs="EPSG:27700",
    )

    with pytest.raises(AssertionError, match="EPSG:27700"):
        prepare_dataset(cfg, force=True)


def test_prepare_dataset_geoparquet_version(tiny_gpkg, tmp_path):
    """prepare_dataset output has GeoParquet 1.1.0 version."""
    out = tmp_path / "hilbert_27700.parquet"
    cfg = DatasetConfig(name="hilbert", source_path=tiny_gpkg, parquet_path=out)
    prepare_dataset(cfg, force=True)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_prepare_dataset_covering_metadata(tiny_gpkg, tmp_path):
    """prepare_dataset output has GeoParquet 1.1 bbox covering key."""
    out = tmp_path / "covering_27700.parquet"
    cfg = DatasetConfig(name="covering", source_path=tiny_gpkg, parquet_path=out)
    prepare_dataset(cfg, force=True)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_prepare_dataset_crs_in_metadata(tiny_gpkg, tmp_path):
    """prepare_dataset patches the CRS into the GeoParquet geometry column metadata."""
    out = tmp_path / "crs_27700.parquet"
    cfg = DatasetConfig(name="crs", source_path=tiny_gpkg, parquet_path=out)
    prepare_dataset(cfg, force=True)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None, "CRS should be present in geometry column metadata"
    # PROJJSON for EPSG:27700 should identify as British National Grid
    assert "27700" in str(crs_meta)


# ---------------------------------------------------------------------------
# prepare_from_csv tests
# ---------------------------------------------------------------------------


@pytest.fixture
def tiny_csv(tmp_path):
    """10-row CSV with Easting/Northing columns in EPSG:27700."""
    p = tmp_path / "tiny.csv"
    with open(p, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "label", "Easting", "Northing"])
        writer.writeheader()
        for i in range(10):
            writer.writerow(
                {
                    "id": i,
                    "label": f"item_{i}",
                    "Easting": 412000 + i * 1000,
                    "Northing": 426000 + i * 1000,
                }
            )
    return p


def test_prepare_from_csv_skips_when_exists(tiny_csv, tmp_path):
    """prepare_from_csv returns without writing when output exists and force=False."""
    out = tmp_path / "csv_out.parquet"
    out.touch()
    result = prepare_from_csv(tiny_csv, out)
    assert result == out
    assert out.stat().st_size == 0  # untouched


def test_prepare_from_csv_writes_parquet(tiny_csv, tmp_path):
    out = tmp_path / "csv_out.parquet"
    result = prepare_from_csv(tiny_csv, out, row_group_size=5)
    assert result == out
    assert out.stat().st_size > 0


def test_prepare_from_csv_geoparquet_version(tiny_csv, tmp_path):
    out = tmp_path / "csv_version.parquet"
    prepare_from_csv(tiny_csv, out)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_prepare_from_csv_covering_metadata(tiny_csv, tmp_path):
    out = tmp_path / "csv_covering.parquet"
    prepare_from_csv(tiny_csv, out)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_prepare_from_csv_crs_in_metadata(tiny_csv, tmp_path):
    out = tmp_path / "csv_crs.parquet"
    prepare_from_csv(tiny_csv, out, crs="EPSG:27700")
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None, "CRS should be present in geometry column metadata"
    assert "27700" in str(crs_meta)


def test_prepare_from_csv_compression_zstd(tiny_csv, tmp_path):
    out = tmp_path / "csv_zstd.parquet"
    prepare_from_csv(tiny_csv, out)
    rg = pq.ParquetFile(out).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        assert col.compression == "ZSTD", (
            f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
        )


def test_prepare_from_csv_xy_cols_dropped(tiny_csv, tmp_path):
    """Source X/Y columns must not appear in the output — replaced by 'geometry'."""
    out = tmp_path / "csv_cols.parquet"
    prepare_from_csv(tiny_csv, out, x_col="Easting", y_col="Northing")
    schema = pq.read_schema(str(out))
    assert "geometry" in schema.names
    assert "Easting" not in schema.names
    assert "Northing" not in schema.names


def test_prepare_from_csv_unsupported_geometry_type_raises(tiny_csv, tmp_path):
    out = tmp_path / "csv_bad.parquet"
    with pytest.raises(NotImplementedError):
        prepare_from_csv(tiny_csv, out, geometry_type="polygon")
