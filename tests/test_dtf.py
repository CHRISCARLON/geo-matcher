"""Tests for usrn_matcher.dtf — DTF8.1-inspired export module."""

import csv as _csv
import json
from datetime import date

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely.wkb
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

from usrn_matcher.dtf import (
    DTFConfig,
    _enc_any,
    _enc_date,
    _enc_int,
    _enc_num,
    _enc_text,
    _geom_type_code,
    _type_10,
    _type_63a,
    _type_67a,
    _type_69,
    _type_99,
    to_dtf_csv,
    to_dtf_flat_csv,
    to_dtf_geoparquet,
    to_dtf_gpkg,
)

pytestmark = pytest.mark.unit
# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cfg() -> DTFConfig:
    return DTFConfig(swa_org_name="Test Council", swa_org_ref=1234, rhs_name="soil")


# ---------------------------------------------------------------------------
# Encoding helpers
# ---------------------------------------------------------------------------


class TestEncText:
    def test_wraps_in_quotes(self):
        assert _enc_text("hello", 40) == '"hello"'

    def test_empty_string(self):
        assert _enc_text("", 40) == '""'

    def test_none(self):
        assert _enc_text(None, 40) == '""'

    def test_truncates_to_max_len(self):
        result = _enc_text("abcde", 3)
        assert result == '"abc"'

    def test_escapes_internal_quotes(self):
        result = _enc_text('say "hello"', 40)
        assert result == '"say ""hello"""'


class TestEncInt:
    def test_simple(self):
        assert _enc_int(7) == "7"

    def test_no_leading_zeros(self):
        assert _enc_int(42) == "42"

    def test_none_returns_empty(self):
        assert _enc_int(None) == ""

    def test_zero(self):
        assert _enc_int(0) == "0"


class TestEncNum:
    def test_two_decimals(self):
        assert _enc_num(412000.5, 2) == "412000.50"

    def test_none_returns_empty(self):
        assert _enc_num(None) == ""

    def test_integer_value(self):
        assert _enc_num(100.0, 2) == "100.00"


class TestEncDate:
    def test_iso_format(self):
        assert _enc_date(date(2026, 4, 13)) == "2026-04-13"

    def test_none_returns_empty(self):
        assert _enc_date(None) == ""


class TestEncAny:
    def test_integer_field(self):
        field = pa.field("x", pa.int32())
        assert _enc_any(42, field) == "42"
        assert _enc_any(None, field) == ""

    def test_float_field(self):
        field = pa.field("x", pa.float64())
        assert _enc_any(3.14, field) == "3.14"

    def test_string_field(self):
        field = pa.field("x", pa.string())
        assert _enc_any("hello", field) == '"hello"'
        assert _enc_any(None, field) == '""'


# ---------------------------------------------------------------------------
# Geometry coordinate extraction
# ---------------------------------------------------------------------------


class TestGeomTypeCode:
    def test_point(self):
        assert _geom_type_code(Point(412000.0, 426000.0)) == "PT"

    def test_linestring(self):
        assert _geom_type_code(LineString([(0.0, 0.0), (1.0, 1.0)])) == "L"

    def test_multilinestring(self):
        assert (
            _geom_type_code(
                MultiLineString([[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]])
            )
            == "ML"
        )

    def test_polygon(self):
        assert _geom_type_code(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])) == "P"

    def test_multipolygon(self):
        mp = MultiPolygon(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 2)]),
            ]
        )
        assert _geom_type_code(mp) == "MP"


# ---------------------------------------------------------------------------
# Record builders
# ---------------------------------------------------------------------------


class TestType10:
    def test_starts_with_10(self, cfg):
        line = _type_10(cfg, date(2026, 4, 13), "120000")
        assert line.startswith("10,")

    def test_contains_org_name(self, cfg):
        line = _type_10(cfg, date(2026, 4, 13), "120000")
        assert '"Test Council"' in line

    def test_contains_org_ref(self, cfg):
        line = _type_10(cfg, date(2026, 4, 13), "120000")
        assert ",1234," in line

    def test_dtf_version_present(self, cfg):
        line = _type_10(cfg, date(2026, 4, 13), "120000")
        assert '"8.1a"' in line

    def test_file_type_f(self, cfg):
        line = _type_10(cfg, date(2026, 4, 13), "120000")
        assert line.endswith('"F"')


class TestType69:
    def test_starts_with_69(self, cfg):
        line = _type_69(cfg, date(2026, 4, 13))
        assert line.startswith("69,")

    def test_contains_rhs_name(self, cfg):
        line = _type_69(cfg, date(2026, 4, 13))
        assert '"soil"' in line

    def test_coord_system(self, cfg):
        line = _type_69(cfg, date(2026, 4, 13))
        assert '"British National Grid"' in line


class TestType63a:
    def test_starts_with_63a(self, cfg):
        line = _type_63a(
            pro_order=1,
            usrn=12345678,
            seq_num=1,
            geom_type_code="P",
            coord_count=5,
            rhs_attr_fields=['"SANDY"', "42"],
            config=cfg,
            today=date(2026, 4, 13),
        )
        assert line.startswith('"63a",')

    def test_point_no_inline_xy(self, cfg):
        """Point geometry coordinates are in type 67a, not inline in type 63a."""
        line = _type_63a(
            pro_order=1,
            usrn=12345678,
            seq_num=1,
            geom_type_code="PT",
            coord_count=1,
            rhs_attr_fields=[],
            config=cfg,
            today=date(2026, 4, 13),
        )
        # Coordinate values must NOT appear in the 63a line
        assert "412000" not in line
        assert "426000" not in line

    def test_asd_coordinate_always_1(self, cfg):
        """ASD_COORDINATE is always 1 — geometry always in type 67a."""
        line = _type_63a(
            pro_order=1,
            usrn=12345678,
            seq_num=1,
            geom_type_code="P",
            coord_count=5,
            rhs_attr_fields=[],
            config=cfg,
            today=date(2026, 4, 13),
        )
        fields = line.split(",")
        # fields[9] = ASD_COORDINATE, fields[10] = ASD_COORDINATE_COUNT
        assert fields[9] == "1"
        assert fields[10] == "5"


class TestType67a:
    def test_starts_with_67a(self):
        line = _type_67a(2, 12345678, 1, "L", "LINESTRING (0 0, 1 1)")
        assert line.startswith('"67a",')

    def test_wkt_embedded(self):
        line = _type_67a(
            2, 12345678, 1, "L", "LINESTRING (412000 426000, 413000 427000)"
        )
        assert "LINESTRING" in line
        assert "412000" in line

    def test_asd_record_identifier_63(self):
        line = _type_67a(2, 12345678, 1, "P", "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
        assert ",63," in line

    def test_point_type_code(self):
        line = _type_67a(1, 12345678, 1, "PT", "POINT (412000 426000)")
        assert '"PT"' in line
        assert "POINT" in line


class TestType99:
    def test_format(self):
        assert _type_99(42) == "99,42"


# ---------------------------------------------------------------------------
# Integration: to_dtf_csv
# ---------------------------------------------------------------------------


def _make_table(geom_wkb: bytes, attrs: dict | None = None) -> pa.Table:
    """Build a minimal result table for testing."""
    data: dict = {
        "usrn": pa.array([12345678], type=pa.int64()),
        "street_type": pa.array(["A Road"], type=pa.string()),
        "geometry": pa.array([b"dummy_wkb"], type=pa.binary()),
        "rhs_geometry": pa.array([geom_wkb], type=pa.binary()),
    }
    if attrs:
        for col, values in attrs.items():
            data[col] = pa.array(values)
    return pa.table(data)


class TestToDtfCsv:
    def test_raises_if_no_rhs_geometry(self, cfg, tmp_path):
        table = pa.table({"usrn": [1], "geometry": [b"x"]})
        with pytest.raises(ValueError, match="rhs_geometry"):
            to_dtf_csv(table, cfg, tmp_path / "out.csv")

    def test_file_structure_point(self, cfg, tmp_path):
        point_wkb = shapely.wkb.dumps(Point(412000.0, 426000.0))
        table = _make_table(point_wkb)
        out = tmp_path / "out.csv"
        to_dtf_csv(table, cfg, out)

        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert lines[0].startswith("10,")
        assert lines[1].startswith("69,")
        assert lines[2].startswith('"63a",')
        assert lines[-1].startswith("99,")
        # Points now emit a type 67a record (not inline in 63a)
        assert any(line.startswith('"67a",') for line in lines)

    def test_file_structure_polygon(self, cfg, tmp_path):
        poly_wkb = shapely.wkb.dumps(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))
        table = _make_table(poly_wkb)
        out = tmp_path / "out.csv"
        to_dtf_csv(table, cfg, out)

        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert lines[0].startswith("10,")
        assert lines[1].startswith("69,")
        assert lines[2].startswith('"63a",')
        # One type 67a record per feature carrying the full WKT
        type_67a_lines = [line for line in lines if line.startswith('"67a",')]
        assert len(type_67a_lines) == 1
        assert "POLYGON" in type_67a_lines[0]
        assert lines[-1].startswith("99,")

    def test_trailer_record_count(self, cfg, tmp_path):
        poly_wkb = shapely.wkb.dumps(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))
        table = _make_table(poly_wkb)
        out = tmp_path / "out.csv"
        to_dtf_csv(table, cfg, out)

        lines = out.read_text(encoding="utf-8").strip().splitlines()
        # Record count = type 69 (1) + type 63a (1) + type 67a (1) = 3
        trailer = lines[-1]
        count = int(trailer.split(",")[1])
        assert count == 3

    def test_rhs_attributes_written(self, cfg, tmp_path):
        point_wkb = shapely.wkb.dumps(Point(412000.0, 426000.0))
        table = _make_table(
            point_wkb,
            attrs={"MAP_SYMBOL": pa.array(["Bs"], type=pa.string())},
        )
        out = tmp_path / "out.csv"
        to_dtf_csv(table, cfg, out)

        content = out.read_text(encoding="utf-8")
        assert '"Bs"' in content

    def test_utf8_encoding(self, cfg, tmp_path):
        point_wkb = shapely.wkb.dumps(Point(0.0, 0.0))
        table = _make_table(
            point_wkb,
            attrs={"name": pa.array(["Café"], type=pa.string())},
        )
        out = tmp_path / "out.csv"
        to_dtf_csv(table, cfg, out)
        content = out.read_bytes().decode("utf-8")
        assert "Café" in content

    def test_multirow_seq_numbers(self, cfg, tmp_path):
        """Two rows with the same USRN should get incrementing seq nums."""
        pt_wkb = shapely.wkb.dumps(Point(0.0, 0.0))
        table = pa.table(
            {
                "usrn": pa.array([12345678, 12345678], type=pa.int64()),
                "street_type": pa.array(["A Road", "A Road"], type=pa.string()),
                "geometry": pa.array([b"x", b"x"], type=pa.binary()),
                "rhs_geometry": pa.array([pt_wkb, pt_wkb], type=pa.binary()),
            }
        )
        out = tmp_path / "out.csv"
        to_dtf_csv(table, cfg, out)

        lines = out.read_text(encoding="utf-8").strip().splitlines()
        type_63a = [line for line in lines if line.startswith('"63a",')]
        assert len(type_63a) == 2
        # ATTRIBUTION_SEQ_NUM is field index 4 (0-based after split)
        seq_nums = [int(line.split(",")[4]) for line in type_63a]
        assert seq_nums == [1, 2]


# ---------------------------------------------------------------------------
# Missing rhs_geometry guard — all four export functions
# ---------------------------------------------------------------------------


class TestMissingRhsGeometry:
    """All four export functions must raise ValueError when rhs_geometry is absent."""

    def _table_without_rhs_geometry(self) -> pa.Table:
        return pa.table({"usrn": [1], "geometry": [b"x"]})

    def test_to_dtf_csv_raises(self, cfg, tmp_path):
        with pytest.raises(ValueError, match="rhs_geometry"):
            to_dtf_csv(self._table_without_rhs_geometry(), cfg, tmp_path / "out.csv")

    def test_to_dtf_geoparquet_raises(self, cfg, tmp_path):
        with pytest.raises(ValueError, match="rhs_geometry"):
            to_dtf_geoparquet(
                self._table_without_rhs_geometry(), cfg, tmp_path / "out.parquet"
            )

    def test_to_dtf_flat_csv_raises(self, cfg, tmp_path):
        with pytest.raises(ValueError, match="rhs_geometry"):
            to_dtf_flat_csv(
                self._table_without_rhs_geometry(), cfg, tmp_path / "out.csv"
            )

    def test_to_dtf_gpkg_raises(self, cfg, tmp_path):
        with pytest.raises(ValueError, match="rhs_geometry"):
            to_dtf_gpkg(self._table_without_rhs_geometry(), cfg, tmp_path / "out.gpkg")


# ---------------------------------------------------------------------------
# Integration: to_dtf_geoparquet output quality
# ---------------------------------------------------------------------------


@pytest.fixture
def multi_row_table():
    """Three-row table with mixed geometry types for output tests."""
    pt_wkb = shapely.wkb.dumps(Point(412000.0, 426000.0))
    ls_wkb = shapely.wkb.dumps(LineString([(412000.0, 426000.0), (413000.0, 427000.0)]))
    poly_wkb = shapely.wkb.dumps(
        Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000), (0, 0)])
    )
    return pa.table(
        {
            "usrn": pa.array([1, 2, 1], type=pa.int64()),
            "street_type": pa.array(["A Road", "B Road", "A Road"], type=pa.string()),
            "geometry": pa.array([b"x", b"x", b"x"], type=pa.binary()),
            "rhs_geometry": pa.array([pt_wkb, ls_wkb, poly_wkb], type=pa.binary()),
            "name": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
        }
    )


class TestToDtfGeoparquet:
    def test_writes_file(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        result = to_dtf_geoparquet(multi_row_table, cfg, out)
        assert result == out
        assert out.stat().st_size > 0

    def test_geoparquet_version_11(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
        assert geo["version"] == "1.1.0"

    def test_covering_metadata(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
        covering = geo["columns"]["geometry"].get("covering", {}).get("bbox", {})
        assert covering == {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }

    def test_crs_in_metadata(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
        crs_meta = geo["columns"]["geometry"].get("crs")
        assert crs_meta is not None
        assert "27700" in str(crs_meta)

    def test_compression_zstd(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        rg = pq.ParquetFile(out).metadata.row_group(0)
        for i in range(rg.num_columns):
            col = rg.column(i)
            assert col.compression == "ZSTD", (
                f"{col.path_in_schema}: expected ZSTD, got {col.compression}"
            )

    def test_dtf_columns_present(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        names = pq.read_schema(str(out)).names
        for col in (
            "USRN",
            "RECORD_IDENTIFIER",
            "ATTRIBUTION_SEQ_NUM",
            "GEOMETRY_TYPE",
            "geometry",
        ):
            assert col in names, f"Expected column {col!r} in GeoParquet output"

    def test_row_count(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        assert pq.read_metadata(str(out)).num_rows == len(multi_row_table)

    def test_rhs_cols_not_in_skip_set_are_preserved(
        self, cfg, multi_row_table, tmp_path
    ):
        """RHS attribute columns (here: 'name') survive into the GeoParquet."""
        out = tmp_path / "out.parquet"
        to_dtf_geoparquet(multi_row_table, cfg, out)
        assert "name" in pq.read_schema(str(out)).names


# ---------------------------------------------------------------------------
# Integration: to_dtf_flat_csv output quality
# ---------------------------------------------------------------------------


class TestToDtfFlatCsv:
    def test_writes_file(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out_flat.csv"
        result = to_dtf_flat_csv(multi_row_table, cfg, out)
        assert result == out
        assert out.stat().st_size > 0

    def test_row_count(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out_flat.csv"
        to_dtf_flat_csv(multi_row_table, cfg, out)
        lines = out.read_text(encoding="utf-8").strip().splitlines()
        # One header + one row per input row
        assert len(lines) == len(multi_row_table) + 1

    def test_dtf_columns_in_header(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out_flat.csv"
        to_dtf_flat_csv(multi_row_table, cfg, out)
        with out.open(encoding="utf-8") as f:
            header = next(_csv.reader(f))
        for col in ("USRN", "RECORD_IDENTIFIER", "GEOMETRY_TYPE", "geometry"):
            assert col in header, f"Expected {col!r} in flat CSV header"

    def test_geometry_col_is_wkt(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out_flat.csv"
        to_dtf_flat_csv(multi_row_table, cfg, out)
        with open(out, newline="", encoding="utf-8") as f:
            rows = list(_csv.DictReader(f))
        # Each geometry cell should be a WKT string, not raw bytes
        for row in rows:
            wkt = row["geometry"]
            assert any(
                wkt.startswith(prefix)
                for prefix in ("POINT", "LINESTRING", "POLYGON", "MULTI")
            ), f"Expected WKT geometry, got: {wkt!r}"

    def test_rhs_attribute_preserved(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out_flat.csv"
        to_dtf_flat_csv(multi_row_table, cfg, out)
        content = out.read_text(encoding="utf-8")
        assert "alpha" in content


# ---------------------------------------------------------------------------
# Integration: to_dtf_gpkg output quality
# ---------------------------------------------------------------------------


class TestToDtfGpkg:
    def test_writes_file(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.gpkg"
        result = to_dtf_gpkg(multi_row_table, cfg, out)
        assert result == out
        assert out.stat().st_size > 0

    def test_readable_as_geodataframe(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.gpkg"
        to_dtf_gpkg(multi_row_table, cfg, out)
        gdf = gpd.read_file(str(out))
        assert len(gdf) == len(multi_row_table)
        assert gdf.crs is not None
        assert gdf.crs.to_epsg() == 27700

    def test_dtf_columns_present(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.gpkg"
        to_dtf_gpkg(multi_row_table, cfg, out)
        gdf = gpd.read_file(str(out))
        for col in ("USRN", "RECORD_IDENTIFIER", "GEOMETRY_TYPE"):
            assert col in gdf.columns, f"Expected column {col!r} in GeoPackage"

    def test_custom_layer_name(self, cfg, multi_row_table, tmp_path):
        out = tmp_path / "out.gpkg"
        to_dtf_gpkg(multi_row_table, cfg, out, layer="my_layer")
        # gpd.list_layers returns a DataFrame with a 'name' column (geopandas ≥ 0.14 / pyogrio)
        layer_names = gpd.list_layers(str(out))["name"].tolist()
        assert "my_layer" in layer_names
