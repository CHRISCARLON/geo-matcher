"""Unit tests for join.py — column fragment generation and bbox helpers."""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from usrn_matcher.config import DatasetConfig
from usrn_matcher.join import _bbox_filter, _bbox_wkt, _col_fragment

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# _bbox_filter
# ---------------------------------------------------------------------------


def test_bbox_filter_none_returns_empty():
    assert _bbox_filter(None) == ""


def test_bbox_filter_produces_where_clause():
    clause = _bbox_filter([100.0, 200.0, 300.0, 400.0])
    assert clause.startswith("WHERE ST_Intersects")
    assert "100.0" in clause
    assert "400.0" in clause


# ---------------------------------------------------------------------------
# _bbox_wkt
# ---------------------------------------------------------------------------


def test_bbox_wkt_none_returns_none():
    assert _bbox_wkt(None) is None


def test_bbox_wkt_produces_polygon():
    wkt: str | None = _bbox_wkt([100.0, 200.0, 300.0, 400.0])
    assert wkt is not None
    assert "POLYGON" in wkt
    assert "ST_SetSRID" in wkt
    assert "27700" in wkt


# ---------------------------------------------------------------------------
# _col_fragment — explicit columns (no parquet file needed)
# ---------------------------------------------------------------------------


def test_col_fragment_explicit_columns():
    cfg: DatasetConfig = DatasetConfig(
        name="soil", source_path="x.gpkg", columns=["MUSID", "MAP_SYMBOL"]
    )
    assert _col_fragment(cfg) == ', s."MUSID", s."MAP_SYMBOL"'


def test_col_fragment_explicit_columns_with_spaces():
    cfg: DatasetConfig = DatasetConfig(
        name="x", source_path="x.gpkg", columns=["road class", "speed limit"]
    )
    assert _col_fragment(cfg) == ', s."road class", s."speed limit"'


# ---------------------------------------------------------------------------
# _col_fragment — auto-discovery from parquet schema
# ---------------------------------------------------------------------------


@pytest.fixture()
def rhs_parquet(tmp_path: pathlib.Path) -> pathlib.Path:
    """Minimal parquet file with columns: id, name, category, geometry, bbox."""
    table: pa.Table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["a", "b"]),
            "category": pa.array(["x", "y"]),
            "geometry": pa.array([b"\x00", b"\x01"]),
            "bbox": pa.array([b"\x00", b"\x01"]),
        }
    )
    out: pathlib.Path = tmp_path / "rhs.parquet"
    pq.write_table(table, str(out))
    return out


def test_col_fragment_auto_excludes_geometry_and_bbox(rhs_parquet: pathlib.Path):
    cfg: DatasetConfig = DatasetConfig(
        name="x", source_path="x.gpkg", parquet_path=rhs_parquet, columns=[]
    )
    result: str = _col_fragment(cfg)
    assert "geometry" not in result
    assert "bbox" not in result
    assert 's."id"' in result
    assert 's."name"' in result
    assert 's."category"' in result


def test_col_fragment_auto_only_geometry_bbox(tmp_path: pathlib.Path):
    """When schema only has geometry and bbox the fragment is an empty trailing comma."""
    table: pa.Table = pa.table(
        {
            "geometry": pa.array([b"\x00"]),
            "bbox": pa.array([b"\x00"]),
        }
    )
    out: pathlib.Path = tmp_path / "geom_only.parquet"
    pq.write_table(table, str(out))

    cfg: DatasetConfig = DatasetConfig(
        name="x", source_path="x.gpkg", parquet_path=out, columns=[]
    )
    assert _col_fragment(cfg) == ", "


def test_col_fragment_auto_reads_parquet_schema(rhs_parquet: pathlib.Path):
    """columns=[] falls through to parquet schema auto-discovery."""
    cfg: DatasetConfig = DatasetConfig(
        name="x", source_path="x.gpkg", parquet_path=rhs_parquet, columns=[]
    )
    result: str = _col_fragment(cfg)
    assert 's."id"' in result
    assert 's."name"' in result
