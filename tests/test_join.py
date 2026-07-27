"""Unit tests for join.py — column fragment generation and bbox helpers."""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from usrn_matcher.config import DatasetConfig, GeometryType
from usrn_matcher.join import (
    _bbox_pruner,
    _col_fragment,
    _registry,
    register,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["hexagon", "", "POINT", 1])
def test_register_rejects_non_geometry_type(name):
    """register() only accepts GeometryType members (or their string values)."""
    with pytest.raises(ValueError, match="not a GeometryType"):
        register(name)


def test_register_accepts_string_value_and_normalises_key():
    """A plain string value registers under the matching GeometryType member."""
    original = dict(_registry)
    try:
        register("point")(lambda *a, **k: None)
        key = next(k for k in _registry if k == GeometryType.POINT)
        assert isinstance(key, GeometryType)
    finally:
        _registry.clear()
        _registry.update(original)


# ---------------------------------------------------------------------------
# _bbox_pruner
# ---------------------------------------------------------------------------


def test_bbox_pruner_produces_where_clause():
    """_bbox_pruner builds an ST_Intersects clause for both sides."""
    clause = _bbox_pruner([100.0, 200.0, 300.0, 400.0])
    assert "ST_Intersects(u.geometry" in clause
    assert "ST_Intersects(s.geometry" in clause
    assert "100.0" in clause
    assert "400.0" in clause


# ---------------------------------------------------------------------------
# _col_fragment — explicit columns (no parquet file needed)
# ---------------------------------------------------------------------------


def test_col_fragment_explicit_columns():
    """Explicit columns are emitted as quoted, s-prefixed fields."""
    cfg: DatasetConfig = DatasetConfig(
        name="soil", source_path="x.gpkg", columns=["MUSID", "MAP_SYMBOL"]
    )
    assert _col_fragment(cfg) == ', s."MUSID", s."MAP_SYMBOL"'


def test_col_fragment_explicit_columns_with_spaces():
    """Column names containing spaces are quoted correctly."""
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
    """Auto-discovery omits the geometry and bbox columns."""
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
