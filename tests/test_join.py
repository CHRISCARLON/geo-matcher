"""Unit tests for join.py — column fragment generation, bbox helpers, post-processors."""

import logging
import pathlib

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from usrn_matcher.config import DatasetConfig, GeometryType
from usrn_matcher.join import (
    _assert_corridor_file_current,
    _bbox_pruner,
    _col_fragment,
    _distinct_ids,
    _log_line_match_summary,
    _nearest_dedup,
    _phase2_select_corridors,
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


# ---------------------------------------------------------------------------
# _phase2_select_corridors — Phase 2 corridor scoring
#
# Geometry fixtures are built so the overlap fractions are exact. The RHS feature is
# a 100 m line; the denominator is GREATEST(line length, 2 * distance_m) = 100. Each
# "corridor" is a rectangle covering a known length of that line:
#
#   FULL_CORRIDOR    covers 100 m → overlap 1.00   (what a Phase 1 street looks like)
#   HALF_CORRIDOR    covers  50 m → overlap 0.50   (a genuine adjacent street)
#   SLIVER_CORRIDOR  covers   5 m → overlap 0.05   (a crossing, below the 10 % floor)
# ---------------------------------------------------------------------------

_MAX_D = 10.0
_FEATURE_LINE = "LINESTRING(0 0, 100 0)"
_FULL_CORRIDOR = "POLYGON((0 -5, 100 -5, 100 5, 0 5, 0 -5))"
_HALF_CORRIDOR = "POLYGON((0 -5, 50 -5, 50 5, 0 5, 0 -5))"
_SLIVER_CORRIDOR = "POLYGON((0 -5, 5 -5, 5 5, 0 5, 0 -5))"


def _wkb(wkt: str) -> bytes:
    """WKB for a WKT literal, via DuckDB's spatial extension."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()[0]


def _corridor_candidates(
    rows: list[tuple[int, str]], feature_id: str = "F1"
) -> pa.Table:
    """Raw Phase 2 join output: one (usrn, corridor WKT) row per candidate street."""
    return pa.table(
        {
            "usrn": pa.array([usrn for usrn, _ in rows], type=pa.int64()),
            "street_type": pa.array(["Designated Street"] * len(rows)),
            "asset_id": pa.array([feature_id] * len(rows)),
            "distance_m": pa.array([0.0] * len(rows), type=pa.float64()),
            "is_intersection": pa.array([False] * len(rows), type=pa.bool_()),
            "_u_geom": pa.array([_wkb(wkt) for _, wkt in rows]),
            "_s_geom": pa.array([_wkb(_FEATURE_LINE)] * len(rows)),
        }
    )


def _phase1_pairs(usrns: list[int], feature_id: str = "F1") -> pa.Table:
    """Stand-in for the Phase 1 result — only asset_id and usrn are read."""
    return pa.table(
        {
            "usrn": pa.array(usrns, type=pa.int64()),
            "street_type": pa.array(["Designated Street"] * len(usrns)),
            "asset_id": pa.array([feature_id] * len(usrns)),
            "distance_m": pa.array([0.0] * len(usrns), type=pa.float64()),
            "is_intersection": pa.array([True] * len(usrns), type=pa.bool_()),
            "overlap_length_pct": pa.array([1.0] * len(usrns), type=pa.float64()),
            "match_phase": pa.array([1] * len(usrns), type=pa.int8()),
        }
    )


def test_phase2_select_corridors_excludes_phase1_pairs():
    """A Phase 1 street must not suppress an adjacent corridor at the 80 % cut.

    This is the regression test for running Phase 2 over the whole slice: USRN 1 was
    already matched by Phase 1, so its ~1.0 self-overlap has to leave the ranking
    window entirely, letting the genuinely adjacent USRN 2 through.
    """
    candidates = _corridor_candidates([(1, _FULL_CORRIDOR), (2, _HALF_CORRIDOR)])

    result = _phase2_select_corridors(
        candidates, "asset_id", _MAX_D, 0.10, exclude_pairs=_phase1_pairs([1])
    )

    assert result.column("usrn").to_pylist() == [2]
    assert result.column("match_phase").to_pylist() == [2]
    assert result.column("overlap_length_pct").to_pylist() == pytest.approx([0.5])


def test_phase2_select_corridors_without_exclude_pairs_is_unchanged():
    """No exclusions → the pre-change behaviour, where the 1.0 overlap wins outright."""
    candidates = _corridor_candidates([(1, _FULL_CORRIDOR), (2, _HALF_CORRIDOR)])

    for exclude in (None, pa.table({"usrn": pa.array([], type=pa.int64())})):
        result = _phase2_select_corridors(
            candidates, "asset_id", _MAX_D, 0.10, exclude_pairs=exclude
        )
        assert result.column("usrn").to_pylist() == [1]


def test_phase2_select_corridors_drops_subthreshold_after_exclusion():
    """Excluding the Phase 1 street doesn't promote a sub-threshold crossing."""
    candidates = _corridor_candidates([(1, _FULL_CORRIDOR), (3, _SLIVER_CORRIDOR)])

    result = _phase2_select_corridors(
        candidates, "asset_id", _MAX_D, 0.10, exclude_pairs=_phase1_pairs([1])
    )

    # USRN 3 scores 0.05, below the 10 % floor, so the feature keeps no Phase 2 match.
    assert len(result) == 0


def test_phase2_select_corridors_excludes_only_matching_feature():
    """Exclusion is per (feature, usrn) pair, not per usrn."""
    candidates = pa.concat_tables(
        [
            _corridor_candidates([(1, _FULL_CORRIDOR)], feature_id="F1"),
            _corridor_candidates([(1, _FULL_CORRIDOR)], feature_id="F2"),
        ]
    )

    result = _phase2_select_corridors(
        candidates,
        "asset_id",
        _MAX_D,
        0.10,
        exclude_pairs=_phase1_pairs([1], feature_id="F1"),
    )

    # F1 already had USRN 1 from Phase 1; F2 did not, so F2 keeps it.
    assert result.column("asset_id").to_pylist() == ["F2"]


def test_phase2_select_corridors_empty_input():
    """An empty candidate set survives the extra CTE."""
    result = _phase2_select_corridors(
        _corridor_candidates([]), "asset_id", _MAX_D, 0.10, exclude_pairs=None
    )
    assert len(result) == 0


def test_phase2_output_concatenates_with_phase3():
    """Phase 2 and Phase 3 results must share a schema — they hit one ParquetWriter."""
    phase2 = _phase2_select_corridors(
        _corridor_candidates([(2, _HALF_CORRIDOR)]), "asset_id", _MAX_D, 0.10
    )
    phase3 = _nearest_dedup(
        pa.table(
            {
                "usrn": pa.array([9], type=pa.int64()),
                "street_type": pa.array(["Designated Street"]),
                "asset_id": pa.array(["F2"]),
                "distance_m": pa.array([4.2], type=pa.float64()),
                "is_intersection": pa.array([False], type=pa.bool_()),
                "overlap_length_pct": pa.array([0.0], type=pa.float64()),
            }
        ),
        "asset_id",
    )

    combined = pa.concat_tables([phase2, phase3])

    assert combined.schema.names == [
        "usrn",
        "street_type",
        "asset_id",
        "distance_m",
        "is_intersection",
        "overlap_length_pct",
        "match_phase",
    ]
    assert combined.schema.field("match_phase").type == pa.int8()


# ---------------------------------------------------------------------------
# _distinct_ids / _log_line_match_summary
# ---------------------------------------------------------------------------


def test_distinct_ids_unions_across_parts():
    """Ids are deduplicated across result tables."""
    parts = [
        pa.table({"asset_id": pa.array(["A", "B", "A"])}),
        pa.table({"asset_id": pa.array(["B", "C"])}),
    ]
    assert _distinct_ids(parts, "asset_id") == {"A", "B", "C"}


def test_log_line_match_summary_counts_each_feature_once(caplog):
    """Phases 1 and 2 overlap now, so `matched` is a union, not a sum."""
    with caplog.at_level(logging.INFO, logger="usrn_matcher"):
        # 10 features: 6 matched at Phase 1, 5 at Phase 2 of which 4 are the same
        # features. Summing would claim 11 matched out of 10.
        _log_line_match_summary(10, 7, 6, 5, 1, 0, 4)

    assert "7/10 RHS features matched (70.0%)" in caplog.text
    assert "of which 4 also Phase 1" in caplog.text
    assert "unmatched: 3" in caplog.text


# ---------------------------------------------------------------------------
# _assert_corridor_file_current
# ---------------------------------------------------------------------------


def _usrn_parquet(path: pathlib.Path, n_rows: int) -> pathlib.Path:
    """Minimal stand-in carrying only the row count the guard reads from the footer."""
    pq.write_table(
        pa.table({"usrn": pa.array(range(n_rows), type=pa.int64())}), str(path)
    )
    return path


def test_corridor_guard_passes_when_row_counts_match(tmp_path: pathlib.Path):
    """The 1:1 case prepare-usrns-line always produces raises nothing."""
    _assert_corridor_file_current(
        _usrn_parquet(tmp_path / "usrns.parquet", 100),
        _usrn_parquet(tmp_path / "usrns_line_10m.parquet", 100),
    )


def test_corridor_guard_raises_on_stale_corridor(tmp_path: pathlib.Path):
    """A corridor file built from an older USRN release must fail loudly.

    Silently, Phase 1 would inner-join away every match for the 5 missing USRNs and
    re-report them as match_phase=2 with is_intersection=false.
    """
    with pytest.raises(ValueError, match="Corridor file is stale"):
        _assert_corridor_file_current(
            _usrn_parquet(tmp_path / "usrns.parquet", 100),
            _usrn_parquet(tmp_path / "usrns_line_10m.parquet", 95),
        )


def test_corridor_guard_message_is_actionable(tmp_path: pathlib.Path):
    """The error names both counts and the command that fixes it."""
    with pytest.raises(ValueError) as exc:
        _assert_corridor_file_current(
            _usrn_parquet(tmp_path / "usrns.parquet", 1766832),
            _usrn_parquet(tmp_path / "usrns_line_10m.parquet", 1700000),
        )
    message = str(exc.value)
    assert "1,766,832" in message
    assert "1,700,000" in message
    assert "prepare-usrns-line" in message
