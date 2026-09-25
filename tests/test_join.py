"""Unit tests for join.py — column fragment generation, bbox helpers, post-processors."""

import logging
import pathlib

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geo_matcher.join import (
    _assert_corridor_file_current,
    _log_line_match_summary,
    _nearest_dedup,
    _phase2_select_corridors,
    bbox_pruner,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# bbox_pruner
# ---------------------------------------------------------------------------


def test_bbox_pruner_produces_where_clause():
    """bbox_pruner builds an ST_Intersects clause for both sides."""
    clause = bbox_pruner([100.0, 200.0, 300.0, 400.0])
    print(clause)
    assert "ST_Intersects(u.geometry" in clause
    assert "ST_Intersects(s.geometry" in clause
    assert "100.0" in clause
    assert "400.0" in clause


# ---------------------------------------------------------------------------
# _phase2_select_corridors — Phase 2 corridor scoring
#
# Geometry fixtures are built so the overlap fractions are exact. The RHS feature is
# a 100 m line; the denominator is GREATEST(line length, 2 * distance_m) = 100. Each
# "corridor" is a rectangle covering a known length of that line:
#
#   FULL_CORRIDOR    covers 100 m → overlap 1.00   (what a Phase 1 street looks like)
#   HALF_CORRIDOR    covers  50 m → overlap 0.50   (a genuine adjacent street)
# ---------------------------------------------------------------------------

_MAX_D = 10.0
_FEATURE_LINE = "LINESTRING(0 0, 100 0)"
_FULL_CORRIDOR = "POLYGON((0 -5, 100 -5, 100 5, 0 5, 0 -5))"
_HALF_CORRIDOR = "POLYGON((0 -5, 50 -5, 50 5, 0 5, 0 -5))"


def _wkb(wkt: str) -> bytes:
    """WKB for a WKT literal, via DuckDB's spatial extension."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()
    assert row is not None  # aggregate-free single-row SELECT always returns a row
    return row[0]


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
# _log_line_match_summary
# ---------------------------------------------------------------------------


def test_log_line_match_summary_counts_each_feature_once(caplog):
    """Phases 1 and 2 overlap now, so `matched` is a union, not a sum."""
    with caplog.at_level(logging.INFO, logger="geo_matcher"):
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
