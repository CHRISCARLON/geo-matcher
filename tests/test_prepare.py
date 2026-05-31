"""Tests for the prepare module: prepare() with source type structs."""

import csv
import json

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

import usrn_matcher.prepare as prepare_module
from usrn_matcher import CsvSource, DatasetConfig, OgrSource
from usrn_matcher.prepare import prepare

pytestmark = pytest.mark.unit

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
    """GeoParquet written from the synthetic GDF via prepare()."""
    out = tmp_path / "test.parquet"
    cfg = DatasetConfig(name="test", source=OgrSource(path=tiny_gpkg), parquet_path=out)
    prepare(cfg, force=True)
    return out


# ---------------------------------------------------------------------------
# OGR source metadata tests
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
# OGR source behaviour tests
# ---------------------------------------------------------------------------


def test_prepare_skips_when_exists(tmp_path):
    """prepare() returns immediately (no file read) when output exists and force=False."""
    out = tmp_path / "fake_27700.parquet"
    out.touch()
    cfg = DatasetConfig(
        name="fake",
        source=OgrSource(path=tmp_path / "does_not_exist.gpkg"),
        parquet_path=out,
    )
    result = prepare(cfg, force=False)
    assert result == out
    assert out.stat().st_size == 0  # untouched


def test_prepare_force_overwrites(tiny_gpkg, tmp_path):
    """prepare() re-writes the file when force=True."""
    out = tmp_path / "forced_27700.parquet"
    out.touch()
    cfg = DatasetConfig(
        name="forced",
        source=OgrSource(path=tiny_gpkg),
        parquet_path=out,
    )
    result = prepare(cfg, force=True)
    assert result == out
    assert out.stat().st_size > 0


def test_prepare_geometry_renamed(tiny_gpkg, tmp_path):
    """DuckDB always outputs the geometry column as 'geometry'."""
    out = tmp_path / "renamed_27700.parquet"
    cfg = DatasetConfig(
        name="renamed", source=OgrSource(path=tiny_gpkg), parquet_path=out
    )
    result = prepare(cfg, force=True)
    schema = pq.read_schema(str(result))
    assert "geometry" in schema.names
    assert "geom" not in schema.names


def test_prepare_wrong_crs_raises(tiny_gdf, tmp_path):
    """ValueError raised when the source CRS doesn't match OgrSource.crs."""
    wrong_crs_gdf = tiny_gdf.to_crs("EPSG:4326")
    src_gpkg = tmp_path / "wrong_crs.gpkg"
    wrong_crs_gdf.to_file(str(src_gpkg), driver="GPKG")
    out = tmp_path / "wrong_crs.parquet"
    cfg = DatasetConfig(
        name="bad",
        source=OgrSource(path=src_gpkg, crs="EPSG:27700"),
        parquet_path=out,
    )
    with pytest.raises(ValueError, match="EPSG:27700"):
        prepare(cfg, force=True)


def test_prepare_geoparquet_version(tiny_gpkg, tmp_path):
    """prepare() output has GeoParquet 1.1.0 version."""
    out = tmp_path / "hilbert_27700.parquet"
    cfg = DatasetConfig(
        name="hilbert", source=OgrSource(path=tiny_gpkg), parquet_path=out
    )
    prepare(cfg, force=True)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_prepare_covering_metadata(tiny_gpkg, tmp_path):
    """prepare() output has GeoParquet 1.1 bbox covering key."""
    out = tmp_path / "covering_27700.parquet"
    cfg = DatasetConfig(
        name="covering", source=OgrSource(path=tiny_gpkg), parquet_path=out
    )
    prepare(cfg, force=True)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_prepare_crs_in_metadata(tiny_gpkg, tmp_path):
    """prepare() patches the CRS into the GeoParquet geometry column metadata."""
    out = tmp_path / "crs_27700.parquet"
    cfg = DatasetConfig(name="crs", source=OgrSource(path=tiny_gpkg), parquet_path=out)
    prepare(cfg, force=True)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None, "CRS should be present in geometry column metadata"
    assert "27700" in str(crs_meta)


# ---------------------------------------------------------------------------
# _patch_covering_metadata unhappy path
# ---------------------------------------------------------------------------


def test_prepare_raises_on_patch_failure(tiny_gpkg, tmp_path, monkeypatch):
    """RuntimeError propagates when _patch_covering_metadata raises."""

    def _always_raise(*a, **kw):
        raise RuntimeError("Failed to patch GeoParquet covering metadata")

    monkeypatch.setattr(prepare_module, "_patch_covering_metadata", _always_raise)
    out = tmp_path / "fail.parquet"
    cfg = DatasetConfig(name="fail", source=OgrSource(path=tiny_gpkg), parquet_path=out)
    with pytest.raises(
        RuntimeError, match="Failed to patch GeoParquet covering metadata"
    ):
        prepare(cfg, force=True)


# ---------------------------------------------------------------------------
# CsvSource tests
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


def test_prepare_csv_skips_when_exists(tiny_csv, tmp_path):
    """prepare() returns without writing when output exists and force=False."""
    out = tmp_path / "csv_out.parquet"
    out.touch()
    cfg = DatasetConfig(name="tiny", source=CsvSource(path=tiny_csv), parquet_path=out)
    result = prepare(cfg)
    assert result == out
    assert out.stat().st_size == 0


def test_prepare_csv_writes_parquet(tiny_csv, tmp_path):
    out = tmp_path / "csv_out.parquet"
    cfg = DatasetConfig(
        name="tiny", source=CsvSource(path=tiny_csv, row_group_size=5), parquet_path=out
    )
    result = prepare(cfg)
    assert result == out
    assert out.stat().st_size > 0


def test_prepare_csv_geoparquet_version(tiny_csv, tmp_path):
    out = tmp_path / "csv_version.parquet"
    cfg = DatasetConfig(name="tiny", source=CsvSource(path=tiny_csv), parquet_path=out)
    prepare(cfg)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_prepare_csv_covering_metadata(tiny_csv, tmp_path):
    out = tmp_path / "csv_covering.parquet"
    cfg = DatasetConfig(name="tiny", source=CsvSource(path=tiny_csv), parquet_path=out)
    prepare(cfg)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_prepare_csv_crs_in_metadata(tiny_csv, tmp_path):
    out = tmp_path / "csv_crs.parquet"
    cfg = DatasetConfig(
        name="tiny", source=CsvSource(path=tiny_csv, crs="EPSG:27700"), parquet_path=out
    )
    prepare(cfg)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None, "CRS should be present in geometry column metadata"
    assert "27700" in str(crs_meta)


def test_prepare_csv_compression_zstd(tiny_csv, tmp_path):
    out = tmp_path / "csv_zstd.parquet"
    cfg = DatasetConfig(name="tiny", source=CsvSource(path=tiny_csv), parquet_path=out)
    prepare(cfg)
    rg = pq.ParquetFile(out).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        assert col.compression == "ZSTD", (
            f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
        )


def test_prepare_csv_xy_cols_dropped(tiny_csv, tmp_path):
    """Source X/Y columns must not appear in the output — replaced by 'geometry'."""
    out = tmp_path / "csv_cols.parquet"
    cfg = DatasetConfig(
        name="tiny",
        source=CsvSource(path=tiny_csv, x_col="Easting", y_col="Northing"),
        parquet_path=out,
    )
    prepare(cfg)
    schema = pq.read_schema(str(out))
    assert "geometry" in schema.names
    assert "Easting" not in schema.names
    assert "Northing" not in schema.names


def test_prepare_csv_unsupported_geometry_type_raises(tiny_csv, tmp_path):
    out = tmp_path / "csv_bad.parquet"
    cfg = DatasetConfig(
        name="tiny",
        source=CsvSource(path=tiny_csv, geometry_type="polygon"),
        parquet_path=out,
    )
    with pytest.raises(NotImplementedError):
        prepare(cfg)
