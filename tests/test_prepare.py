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
