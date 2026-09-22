"""Unit tests for the prepare module: prepare() with source type structs."""

import csv
import json
import pathlib

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

import geo_matcher.prepare as prepare_module
from geo_matcher import CsvSource, DatasetConfig, OgrSource, UprnSource, UsrnSource
from geo_matcher.prepare import prepare

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


def test_geoparquet_metadata_patch(prepared_parquet):
    """GeoParquet metadata is patched to version 1.1.0."""
    geo = json.loads(pq.read_schema(prepared_parquet).metadata[b"geo"])
    print(json.dumps(geo, indent=2))
    assert geo["version"] == "1.1.0"


def test_covering_metadata(prepared_parquet):
    """GeoParquet metadata includes the bbox covering struct."""
    geo = json.loads(pq.read_schema(prepared_parquet).metadata[b"geo"])
    covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
    assert covering == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


def test_compression_zstd(prepared_parquet):
    """All columns are ZSTD-compressed."""
    rg = pq.ParquetFile(prepared_parquet).metadata.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)

        print(f"The compression is: {col.compression}")

        assert col.compression == "ZSTD", (
            f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
        )


def test_crs_in_metadata(prepared_parquet):
    """CRS is present in the geometry column metadata."""
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


def test_prepare_ogr_reprojects_mismatched_crs(tmp_path):
    """A source in a different (but detectable) CRS is reprojected to OgrSource.crs."""
    import duckdb

    # Small boxes near central London, expressed in real-world WGS84 lon/lat —
    # away from the BNG grid origin, where round-trip transform error is largest.
    base_lon, base_lat = -0.1276, 51.5074
    geoms = [
        box(
            base_lon + i * 0.001,
            base_lat + i * 0.001,
            base_lon + i * 0.001 + 0.0005,
            base_lat + i * 0.001 + 0.0005,
        )
        for i in range(10)
    ]
    wgs84_gdf = gpd.GeoDataFrame({"val": range(10), "geometry": geoms}, crs="EPSG:4326")
    src_gpkg = tmp_path / "wgs84.gpkg"
    wgs84_gdf.to_file(str(src_gpkg), driver="GPKG")
    out = tmp_path / "wgs84_27700.parquet"
    cfg = DatasetConfig(
        name="reprojected",
        source=OgrSource(path=src_gpkg, crs="EPSG:27700"),
        parquet_path=out,
    )
    prepare(cfg, force=True)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.sql(f"""
        SELECT MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry))
        FROM read_parquet('{out}')
    """).fetchone()
    assert row is not None
    xmin, ymin = row
    # Reprojected into the BNG extent around central London (~530000, ~180000),
    # not left in WGS84 lon/lat degrees (which would be ~ -0.13 / ~51.5).
    assert 500_000 <= xmin <= 560_000
    assert 150_000 <= ymin <= 210_000

    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None
    assert "27700" in str(crs_meta)


def test_prepare_ogr_undetectable_crs_raises(tiny_gdf, tmp_path, monkeypatch):
    """ValueError raised when the source CRS cannot be detected at all."""
    src_gpkg = tmp_path / "tiny.gpkg"
    tiny_gdf.to_file(str(src_gpkg), driver="GPKG")
    out = tmp_path / "undetectable.parquet"
    cfg = DatasetConfig(
        name="undetectable",
        source=OgrSource(path=src_gpkg, crs="EPSG:27700"),
        parquet_path=out,
    )

    def _fake_read_ogr_info(con, source_path):
        return {"crs": None, "feature_count": 10, "geometry_type": "Polygon"}

    monkeypatch.setattr(prepare_module, "_read_ogr_info", _fake_read_ogr_info)
    with pytest.raises(ValueError, match="Could not detect source CRS"):
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
# UsrnSource tests
# ---------------------------------------------------------------------------


@pytest.fixture
def tiny_usrn_gdf():
    """5-row synthetic USRN-shaped GeoDataFrame — usrn, street_type, LineString geometry."""
    from shapely.geometry import LineString

    geoms = [
        LineString([(i * 1000, i * 1000), (i * 1000 + 500, i * 1000 + 500)])
        for i in range(5)
    ]
    return gpd.GeoDataFrame(
        {
            "usrn": range(10000, 10005),
            "street_type": ["Named Road"] * 5,
            "geometry": geoms,
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def tiny_usrn_gpkg(tiny_usrn_gdf, tmp_path):
    """Write tiny_usrn_gdf to a real GeoPackage — input for UsrnSource plain-mode tests."""
    p = tmp_path / "tiny_usrn.gpkg"
    tiny_usrn_gdf.to_file(str(p), driver="GPKG")
    return p


@pytest.fixture
def tiny_usrn_parquet(tiny_usrn_gpkg, tmp_path):
    """Prepared USRN centreline GeoParquet (UsrnSource plain mode) — input for buffered-mode tests."""
    out = tmp_path / "tiny_usrns_27700.parquet"
    cfg = DatasetConfig(
        name="tiny_usrns", source=UsrnSource(path=tiny_usrn_gpkg), parquet_path=out
    )
    prepare(cfg, force=True)
    return out


def test_prepare_usrn_plain_mode_matches_ogr(tiny_usrn_gpkg, tmp_path):
    """UsrnSource(buffer_m=None) behaves like _prepare_ogr — plain centreline GeoParquet."""
    out = tmp_path / "usrn_plain_27700.parquet"
    cfg = DatasetConfig(
        name="usrn_plain", source=UsrnSource(path=tiny_usrn_gpkg), parquet_path=out
    )
    result = prepare(cfg, force=True)
    schema = pq.read_schema(str(result))
    assert "geometry" in schema.names
    assert "geometry_line" not in schema.names  # plain mode never adds this column

    geo = json.loads(schema.metadata[b"geo"])
    assert geo["version"] == "1.1.0"
    assert "27700" in str(geo["columns"]["geometry"].get("crs"))


def test_prepare_usrn_buffered_mode_adds_geometry_line(tiny_usrn_parquet, tmp_path):
    """UsrnSource(buffer_m=10.0) buffers `geometry`, keeps the original in `geometry_line`."""
    out = tmp_path / "usrn_buffered_27700.parquet"
    cfg = DatasetConfig(
        name="usrn_buffered",
        source=UsrnSource(path=tiny_usrn_parquet, buffer_m=10.0),
        parquet_path=out,
    )
    result = prepare(cfg, force=True)
    schema = pq.read_schema(str(result))
    assert "geometry" in schema.names
    assert "geometry_line" in schema.names


def test_prepare_usrn_buffered_mode_geometry_larger_than_line(
    tiny_usrn_parquet, tmp_path
):
    """The buffered `geometry` column's bbox is strictly larger than `geometry_line`'s."""
    import duckdb

    out = tmp_path / "usrn_buffered_bbox_27700.parquet"
    cfg = DatasetConfig(
        name="usrn_buffered_bbox",
        source=UsrnSource(path=tiny_usrn_parquet, buffer_m=10.0),
        parquet_path=out,
    )
    prepare(cfg, force=True)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.sql(f"""
        SELECT
            MIN(ST_XMin(geometry)), MAX(ST_XMax(geometry)),
            MIN(ST_XMin(geometry_line)), MAX(ST_XMax(geometry_line))
        FROM read_parquet('{out}')
    """).fetchone()
    assert row is not None  # aggregate query always returns exactly one row
    buf_xmin, buf_xmax, line_xmin, line_xmax = row

    assert (buf_xmax - buf_xmin) > (line_xmax - line_xmin)


# ---------------------------------------------------------------------------
# UprnSource tests
# ---------------------------------------------------------------------------


@pytest.fixture
def tiny_uprn_gdf():
    """5-row synthetic UPRN-shaped GeoDataFrame — uppercase columns, Point geometry.

    Column names/casing mirror the real OS Open UPRN GeoPackage (``UPRN``,
    ``X_COORDINATE``, ``Y_COORDINATE``, ``LATITUDE``, ``LONGITUDE``).
    """
    from shapely.geometry import Point

    xs = [i * 1000 for i in range(5)]
    ys = [i * 1000 for i in range(5)]
    return gpd.GeoDataFrame(
        {
            "UPRN": range(100000, 100005),
            "X_COORDINATE": xs,
            "Y_COORDINATE": ys,
            "LATITUDE": [51.5 + i * 0.01 for i in range(5)],
            "LONGITUDE": [-0.1 + i * 0.01 for i in range(5)],
            "geometry": [Point(x, y) for x, y in zip(xs, ys)],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def tiny_uprn_gpkg(tiny_uprn_gdf, tmp_path):
    """Write tiny_uprn_gdf to a real GeoPackage — input for UprnSource plain-mode tests."""
    p = tmp_path / "tiny_uprn.gpkg"
    tiny_uprn_gdf.to_file(str(p), driver="GPKG")
    return p


@pytest.fixture
def tiny_uprn_parquet(tiny_uprn_gpkg, tmp_path):
    """Prepared UPRN address-point GeoParquet (UprnSource plain mode) — input for buffered-mode tests."""
    out = tmp_path / "tiny_uprns_27700.parquet"
    cfg = DatasetConfig(
        name="tiny_uprns", source=UprnSource(path=tiny_uprn_gpkg), parquet_path=out
    )
    prepare(cfg, force=True)
    return out


def test_prepare_uprn_plain_mode_minimal_columns(tiny_uprn_gpkg, tmp_path):
    """UprnSource(buffer_m=None) keeps only `uprn` + `geometry` (+ bbox) — id lowercased,
    x/y/lat/lon dropped as redundant with geometry."""
    out = tmp_path / "uprn_plain_27700.parquet"
    cfg = DatasetConfig(
        name="uprn_plain", source=UprnSource(path=tiny_uprn_gpkg), parquet_path=out
    )
    result = prepare(cfg, force=True)
    schema = pq.read_schema(str(result))
    assert set(schema.names) == {"uprn", "geometry", "bbox"}

    geo = json.loads(schema.metadata[b"geo"])
    assert geo["version"] == "1.1.0"
    assert "27700" in str(geo["columns"]["geometry"].get("crs"))


def test_prepare_uprn_buffered_mode_adds_geometry_point(tiny_uprn_parquet, tmp_path):
    """UprnSource(buffer_m=10.0) buffers `geometry`, keeps the original in `geometry_point`."""
    out = tmp_path / "uprn_buffered_27700.parquet"
    cfg = DatasetConfig(
        name="uprn_buffered",
        source=UprnSource(path=tiny_uprn_parquet, buffer_m=10.0),
        parquet_path=out,
    )
    result = prepare(cfg, force=True)
    schema = pq.read_schema(str(result))
    assert set(schema.names) == {"uprn", "geometry", "geometry_point", "bbox"}


def test_prepare_uprn_buffered_mode_geometry_larger_than_point(
    tiny_uprn_parquet, tmp_path
):
    """The buffered `geometry` column's bbox is strictly larger than `geometry_point`'s."""
    import duckdb

    out = tmp_path / "uprn_buffered_bbox_27700.parquet"
    cfg = DatasetConfig(
        name="uprn_buffered_bbox",
        source=UprnSource(path=tiny_uprn_parquet, buffer_m=10.0),
        parquet_path=out,
    )
    prepare(cfg, force=True)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.sql(f"""
        SELECT
            MIN(ST_XMin(geometry)), MAX(ST_XMax(geometry)),
            MIN(ST_XMin(geometry_point)), MAX(ST_XMax(geometry_point))
        FROM read_parquet('{out}')
    """).fetchone()
    assert row is not None  # aggregate query always returns exactly one row
    buf_xmin, buf_xmax, pt_xmin, pt_xmax = row

    assert (buf_xmax - buf_xmin) > (pt_xmax - pt_xmin)


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
    """prepare() writes a non-empty parquet file from a CSV source."""
    cfg = DatasetConfig(
        name="tiny", source=CsvSource(path=tiny_csv, row_group_size=5), parquet_path=out
    )
    result = prepare(cfg)
    assert result == out
    assert out.stat().st_size > 0


def test_prepare_csv_geoparquet_version(tiny_csv, tmp_path):
    """CSV output has GeoParquet 1.1.0 version."""
    out = tmp_path / "csv_version.parquet"
    cfg = DatasetConfig(name="tiny", source=CsvSource(path=tiny_csv), parquet_path=out)
    prepare(cfg)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    assert geo["version"] == "1.1.0"


def test_prepare_csv_covering_metadata(tiny_csv, tmp_path):
    """CSV output has the GeoParquet 1.1 bbox covering key."""
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
    """CSV output patches the CRS into the geometry column metadata."""
    out = tmp_path / "csv_crs.parquet"
    cfg = DatasetConfig(
        name="tiny", source=CsvSource(path=tiny_csv, crs="EPSG:27700"), parquet_path=out
    )
    prepare(cfg)
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None, "CRS should be present in geometry column metadata"
    assert "27700" in str(crs_meta)


@pytest.fixture
def tiny_lonlat_csv(tmp_path):
    """10-row CSV with lon/lat coordinate columns in EPSG:4326 (near the UK)."""
    p = tmp_path / "tiny_lonlat.csv"
    with open(p, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "lon", "lat"])
        writer.writeheader()
        for i in range(10):
            writer.writerow({"id": i, "lon": -1.0 + i * 0.01, "lat": 51.0 + i * 0.01})
    return p


def test_prepare_csv_reprojects_source_crs(tiny_lonlat_csv, tmp_path):
    """A CsvSource.source_crs different from crs is reprojected during prepare()."""
    import duckdb

    out = tmp_path / "csv_lonlat_27700.parquet"
    cfg = DatasetConfig(
        name="lonlat",
        source=CsvSource(
            path=tiny_lonlat_csv,
            x_col="lon",
            y_col="lat",
            source_crs="EPSG:4326",
            crs="EPSG:27700",
        ),
        parquet_path=out,
    )
    prepare(cfg, force=True)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.sql(f"""
        SELECT MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry))
        FROM read_parquet('{out}')
    """).fetchone()
    assert row is not None
    xmin, ymin = row
    assert 0 <= xmin <= 700_000
    assert 0 <= ymin <= 1_300_000

    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs_meta = geo["columns"]["geometry"].get("crs")
    assert crs_meta is not None
    assert "27700" in str(crs_meta)


def test_prepare_csv_compression_zstd(tiny_csv, tmp_path):
    """CSV output is ZSTD-compressed across all columns."""
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


@pytest.mark.parametrize("geometry_type", ["line", "polygon"])
def test_prepare_csv_wkt_col_required_raises(geometry_type):
    """geometry_type='line'/'polygon' without wkt_col is rejected at construction time,
    before prepare() is ever called."""
    with pytest.raises(ValueError, match="wkt_col"):
        CsvSource(path=pathlib.Path("does_not_matter.csv"), geometry_type=geometry_type)


# ---------------------------------------------------------------------------
# CsvSource LINE/POLYGON (WKT) tests
# ---------------------------------------------------------------------------


@pytest.fixture
def tiny_line_csv(tmp_path):
    """5-row CSV with a WKT column of LINESTRING/MULTILINESTRING text."""
    p = tmp_path / "tiny_line.csv"
    with open(p, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "wkt"])
        writer.writeheader()
        for i in range(4):
            writer.writerow({"id": i, "wkt": f"LINESTRING({i} {i}, {i + 1} {i + 1})"})
        writer.writerow({"id": 4, "wkt": "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"})
    return p


@pytest.fixture
def tiny_polygon_csv(tmp_path):
    """5-row CSV with a WKT column of POLYGON/MULTIPOLYGON text."""
    p = tmp_path / "tiny_polygon.csv"
    with open(p, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "wkt"])
        writer.writeheader()
        for i in range(4):
            writer.writerow(
                {
                    "id": i,
                    "wkt": (
                        f"POLYGON(({i} {i}, {i + 1} {i}, {i + 1} {i + 1}, "
                        f"{i} {i + 1}, {i} {i}))"
                    ),
                }
            )
        writer.writerow(
            {
                "id": 4,
                "wkt": (
                    "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), "
                    "((2 2, 3 2, 3 3, 2 3, 2 2)))"
                ),
            }
        )
    return p


def test_prepare_csv_line_with_wkt_col(tiny_line_csv, tmp_path):
    """CSV line geometries build via ST_GeomFromText and drop wkt_col from the output."""
    out = tmp_path / "csv_line.parquet"
    cfg = DatasetConfig(
        name="tiny_line",
        source=CsvSource(path=tiny_line_csv, geometry_type="line", wkt_col="wkt"),
        parquet_path=out,
    )
    result = prepare(cfg)
    assert result == out
    schema = pq.read_schema(str(out))
    assert "geometry" in schema.names
    assert "wkt" not in schema.names


def test_prepare_csv_polygon_with_wkt_col(tiny_polygon_csv, tmp_path):
    """CSV polygon geometries build via ST_GeomFromText and drop wkt_col from the output."""
    out = tmp_path / "csv_polygon.parquet"
    cfg = DatasetConfig(
        name="tiny_polygon",
        source=CsvSource(path=tiny_polygon_csv, geometry_type="polygon", wkt_col="wkt"),
        parquet_path=out,
    )
    result = prepare(cfg)
    assert result == out
    schema = pq.read_schema(str(out))
    assert "geometry" in schema.names
    assert "wkt" not in schema.names


def test_prepare_csv_line_multilinestring_roundtrips(tiny_line_csv, tmp_path):
    """LINESTRING and MULTILINESTRING WKT both survive the ST_GeomFromText → WKB round-trip."""
    out = tmp_path / "csv_multiline.parquet"
    cfg = DatasetConfig(
        name="tiny_line",
        source=CsvSource(path=tiny_line_csv, geometry_type="line", wkt_col="wkt"),
        parquet_path=out,
    )
    prepare(cfg)
    gdf = gpd.read_parquet(out)
    assert set(gdf.geometry.geom_type) == {"LineString", "MultiLineString"}


def test_prepare_csv_polygon_multipolygon_roundtrips(tiny_polygon_csv, tmp_path):
    """POLYGON and MULTIPOLYGON WKT both survive the ST_GeomFromText → WKB round-trip."""
    out = tmp_path / "csv_multipolygon.parquet"
    cfg = DatasetConfig(
        name="tiny_polygon",
        source=CsvSource(path=tiny_polygon_csv, geometry_type="polygon", wkt_col="wkt"),
        parquet_path=out,
    )
    prepare(cfg)
    gdf = gpd.read_parquet(out)
    assert set(gdf.geometry.geom_type) == {"Polygon", "MultiPolygon"}


# ---------------------------------------------------------------------------
# Connection lifecycle, memory limit, and OGR staging
#
# These cover the fix for the exit-139 (SIGSEGV) crashes seen preparing large
# national GeoPackages on CI runners.
# ---------------------------------------------------------------------------


def test_prepare_leaves_no_open_duckdb_connection(tiny_gpkg, tmp_path):
    """prepare() must not leak its in-memory DuckDB database.

    Each duckdb.connect() is an independent instance with its own memory budget
    (~80% of RAM by default), so a process calling prepare() several times used to
    accumulate one live instance per call.
    """
    import gc

    import duckdb

    before = sum(
        1 for o in gc.get_objects() if isinstance(o, duckdb.DuckDBPyConnection)
    )

    cfg = DatasetConfig(
        name="t",
        source=OgrSource(path=tiny_gpkg),
        parquet_path=tmp_path / "t.parquet",
    )
    prepare(cfg, force=True)

    gc.collect()
    after = sum(1 for o in gc.get_objects() if isinstance(o, duckdb.DuckDBPyConnection))
    assert after == before


def test_memory_limit_is_applied_to_the_connection(monkeypatch):
    """A caller-supplied memory_limit reaches the DuckDB session."""
    capped = prepare_module._open_connection(threads=1, memory_limit="512MB")
    uncapped = prepare_module._open_connection(threads=1)
    try:
        # DuckDB restates the limit in MiB ("488.2 MiB" for 512MB), so compare the
        # capped session against the default rather than matching on the string.
        got = capped.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        default = uncapped.execute("SELECT current_setting('memory_limit')").fetchone()[
            0
        ]

        assert "MiB" in got
        assert float(got.split()[0]) < 600  # ~488 MiB
        assert got != default
    finally:
        capped.close()
        uncapped.close()


def test_connection_context_manager_closes_on_error():
    """_connection() closes even when the body raises."""
    import duckdb

    captured = {}
    with pytest.raises(RuntimeError):
        with prepare_module._connection(threads=1) as con:
            captured["con"] = con
            raise RuntimeError("boom")

    with pytest.raises(duckdb.ConnectionException):
        captured["con"].execute("SELECT 1")


def test_ogr_staging_file_is_cleaned_up(tiny_gpkg, tmp_path):
    """The intermediate staging parquet must not survive a successful prepare."""
    out = tmp_path / "staged_out.parquet"
    cfg = DatasetConfig(name="t", source=OgrSource(path=tiny_gpkg), parquet_path=out)
    prepare(cfg, force=True)

    assert out.exists()
    assert not (out.parent / f"{out.stem}.staging.parquet").exists()
    assert list(tmp_path.glob("*.staging.parquet")) == []


def test_ogr_staging_preserves_rows_columns_and_geometry(tiny_gdf, tiny_gpkg, tmp_path):
    """Routing the OGR read through a staging parquet must not change the output.

    Geometry crosses the staging file as WKB, so this asserts the round trip is
    lossless for both the attribute columns and the coordinates.
    """
    out = tmp_path / "t.parquet"
    cfg = DatasetConfig(name="t", source=OgrSource(path=tiny_gpkg), parquet_path=out)
    prepare(cfg, force=True)

    got = gpd.read_parquet(out)

    assert len(got) == len(tiny_gdf)
    assert {"val", "category", "geometry"} <= set(got.columns)
    assert sorted(got["val"].tolist()) == sorted(tiny_gdf["val"].tolist())

    # geopandas consumes the bbox covering column on read, so assert it on the
    # parquet schema instead — row-group pruning depends on it surviving staging.
    assert "bbox" in pq.read_schema(out).names

    # same geometries, regardless of the Hilbert ordering
    expected_area = sorted(round(g.area, 6) for g in tiny_gdf.geometry)
    actual_area = sorted(round(g.area, 6) for g in got.geometry)
    assert actual_area == expected_area


@pytest.fixture
def curve_gpkg(tmp_path):
    """A GeoPackage carrying one CircularString alongside three usable features.

    DuckDB's GEOMETRY cannot represent the curve WKB types, so this is the shape
    that used to abort the whole staging scan with "Unsupported geometry type in
    WKB". GDAL will not write a curve from DuckDB or GeoPandas, so the feature is
    inserted as a raw GPKG blob through SQLite (the GPKG validation triggers call
    spatialite functions that plain SQLite lacks, hence dropping them first).
    """
    import sqlite3
    import struct

    import duckdb

    p = tmp_path / "curves.gpkg"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"""
        COPY (
            SELECT 1 AS usrn, ST_GeomFromText('LINESTRING Z (0 0 5, 1 1 6)') AS geom
            UNION ALL SELECT 2, ST_GeomFromText('MULTILINESTRING ZM ((0 0 1 2, 1 1 3 4))')
            UNION ALL SELECT 3, ST_GeomFromText('LINESTRING (0 0, 1 1)')
        ) TO '{p}' WITH (FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:27700')
    """)
    con.close()

    # WKB type code 8 == CircularString, wrapped in the GPKG blob header.
    wkb = b"\x01" + struct.pack("<I", 8) + struct.pack("<I", 3)
    for x, y in [(0, 0), (1, 1), (2, 0)]:
        wkb += struct.pack("<dd", x, y)
    blob = b"GP" + bytes([0, 1]) + struct.pack("<i", 27700) + wkb

    db = sqlite3.connect(p)
    for (trigger,) in db.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger'"
    ).fetchall():
        db.execute(f'DROP TRIGGER "{trigger}"')
    db.execute("INSERT INTO curves (usrn, geom) VALUES (?, ?)", (99, blob))
    db.commit()
    db.close()
    return p


def test_unsupported_curve_geometry_is_dropped_not_fatal(curve_gpkg, tmp_path, caplog):
    """One curve feature must not abort the prepare of a whole national file.

    DuckDB has no representation for the curve/surface WKB types and no function
    that linearises them, so they cannot reach GeoParquet at all. They are dropped
    and reported rather than raising, which is what a single such feature in an OS
    "Unknown (any)" layer used to do to the entire staging scan.
    """
    out = tmp_path / "curves.parquet"
    cfg = DatasetConfig(
        name="curves", source=OgrSource(path=curve_gpkg), parquet_path=out
    )

    with caplog.at_level("WARNING"):
        prepare(cfg, force=True)

    got = gpd.read_parquet(out)
    assert sorted(got["usrn"].tolist()) == [1, 2, 3]  # the curve feature (99) dropped

    warning = "\n".join(r.getMessage() for r in caplog.records)
    assert "1 of 4 features" in warning
    assert "CircularString" in warning
    assert "CONVERT_TO_LINEAR" in warning


def test_ogr_geometry_is_forced_to_2d(curve_gpkg, tmp_path):
    """Z/M ordinates are dropped so one column never mixes XY and XYZ geometries."""
    out = tmp_path / "flat.parquet"
    cfg = DatasetConfig(
        name="flat", source=OgrSource(path=curve_gpkg), parquet_path=out
    )
    prepare(cfg, force=True)

    got = gpd.read_parquet(out)
    assert not got.geometry.has_z.any()


def test_wkb_type_name_decodes_both_dialects():
    """_wkb_type_name must read ISO and old-OGC/EWKB type codes, and both endians."""
    name = prepare_module._wkb_type_name

    assert name("0102000000") == "LineString"  # little-endian ISO
    assert name("01ea030000") == "LineString Z"  # ISO 1002
    assert name("01ba0b0000") == "LineString ZM"  # ISO 3002
    assert name("0102000080") == "LineString Z"  # EWKB high-bit Z
    assert name("0108000000") == "CircularString"
    assert name("0000000002") == "LineString"  # big-endian
    assert name("zz") == "unknown"


@pytest.mark.parametrize(
    "source_factory",
    [
        pytest.param(lambda p: OgrSource(path=p), id="ogr"),
        pytest.param(lambda p: UsrnSource(path=p), id="usrn"),
        pytest.param(lambda p: UprnSource(path=p), id="uprn"),
    ],
)
def test_memory_limit_is_forwarded_through_dispatch(
    tiny_gpkg, tmp_path, monkeypatch, source_factory
):
    """prepare()'s memory_limit must survive dispatch to every _prepare_* branch.

    It used to be accepted and silently dropped, so a caller capping DuckDB on a
    constrained runner still got the default budget of ~80% of system RAM.
    """
    seen = {}
    original = prepare_module._open_connection

    def spy(threads=None, memory_limit=None):
        seen["threads"] = threads
        seen["memory_limit"] = memory_limit
        return original(threads, memory_limit)

    monkeypatch.setattr(prepare_module, "_open_connection", spy)

    # UPRN's plain path needs the uppercase UPRN id column the OS source ships.
    gdf = gpd.read_file(tiny_gpkg)
    gdf["UPRN"] = range(len(gdf))
    src_path = tmp_path / "with_uprn.gpkg"
    gdf.to_file(str(src_path), driver="GPKG")

    cfg = DatasetConfig(
        name="t",
        source=source_factory(src_path),
        parquet_path=tmp_path / "mem.parquet",
    )
    prepare(cfg, force=True, threads=2, memory_limit="512MB")

    assert seen == {"threads": 2, "memory_limit": "512MB"}


def test_importing_geo_matcher_does_not_load_native_geo_stacks():
    """geo_matcher must be importable without pulling in sedonadb or pyogrio.

    Those load their own GDAL/native libraries. The prepare path drives DuckDB's
    GDAL hard, and co-resident native geo stacks are the suspected cause of the
    SIGSEGV; callers that only prepare() should not pay for the matcher's deps.
    """
    import subprocess
    import sys

    code = (
        "import sys, geo_matcher; "
        "assert 'sedona.db' not in sys.modules, 'sedona.db imported'; "
        "assert not any(m.startswith('sedonadb') for m in sys.modules), 'sedonadb imported'; "
        "assert 'pyogrio' not in sys.modules, 'pyogrio imported'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
