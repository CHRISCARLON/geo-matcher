"""Unit tests for UsrnMatcher match dispatch and output writing."""

import pyarrow as pa
import pytest

from usrn_matcher import DatasetConfig, UsrnMatcher
from usrn_matcher.join import (
    FilteredMode,
    JoinFn,
    _registry,
    get_join,
)


@pytest.fixture()
def matcher(tmp_path) -> UsrnMatcher:
    """A UsrnMatcher wired to throwaway parquet paths under tmp_path."""
    cfg = DatasetConfig(
        name="test",
        source_path=tmp_path / "test.parquet",
        parquet_path=tmp_path / "test.parquet",
    )
    return UsrnMatcher(usrn_parquet=tmp_path / "usrns_27700.parquet", rhs_config=cfg)


def test_match_dispatch_unknown_mode_raises(matcher):
    """match_dispatch() rejects an unregistered join mode."""
    with pytest.raises(ValueError, match="Unknown join mode"):
        matcher.match_dispatch(mode="fuzzy")


def test_output_writer_unknown_format_raises(matcher, tmp_path):
    """output_writer() rejects an unsupported output format."""
    with pytest.raises(ValueError, match="Unknown output format"):
        matcher.output_writer(pa.table({}), "xlsx", tmp_path, "stem")


def test_registry_contains_expected_modes():
    """The join registry exposes the polygon, point and line modes."""
    assert "polygon" in _registry
    assert "point" in _registry
    assert "line" in _registry
    assert all(isinstance(get_join(k), JoinFn) for k in _registry)


def test_filtered_mode_rejects_oversized_bbox():
    """FilteredMode rejects a bbox above the area limit."""
    # 200km × 200km = 40,000 km² — well above the 3,000 km² limit
    with pytest.raises(ValueError, match="km².*limit"):
        FilteredMode(bbox=(0, 0, 200_000, 200_000))


def test_filtered_mode_accepts_city_sized_bbox():
    """FilteredMode accepts a city-sized bbox under the area limit."""
    # London bbox — should be just under the limit
    FilteredMode(bbox=(503000, 156000, 562000, 201000))
