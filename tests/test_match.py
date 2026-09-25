"""Unit tests for GeoMatcher match dispatch and output writing."""

import pyarrow as pa
import pytest

from geo_matcher import DatasetConfig, GeoMatcher, GeometryType, LhsKind
from geo_matcher.join import (
    FilteredMode,
    JoinFn,
    _registry,
    get_join,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def matcher(tmp_path) -> GeoMatcher:
    """A GeoMatcher wired to throwaway parquet paths under tmp_path."""
    cfg = DatasetConfig(
        name="test",
        source_path=tmp_path / "test.parquet",
        parquet_path=tmp_path / "test.parquet",
    )
    return GeoMatcher(usrn_parquet=tmp_path / "usrns_27700.parquet", rhs_config=cfg)


def test_match_dispatch_routes_uprn_polygon(monkeypatch, matcher):
    """match_dispatch(lhs='uprn', mode='polygon') resolves and calls the registered UPRN join."""
    captured: dict = {}

    def _fake_uprn_join(sd, usrn_parquet, rhs_config, **kwargs):
        captured["sd"] = sd
        captured["usrn_parquet"] = usrn_parquet
        captured["rhs_config"] = rhs_config
        return pa.table({})

    monkeypatch.setitem(
        _registry, (LhsKind.UPRN, GeometryType.POLYGON), _fake_uprn_join
    )
    sentinel_sd = object()
    monkeypatch.setattr(GeoMatcher, "_connect", lambda self: sentinel_sd)

    matcher.match_dispatch(mode="polygon", lhs="uprn")

    assert captured["sd"] is sentinel_sd
    assert captured["usrn_parquet"] == matcher._usrn_parquet
    assert captured["rhs_config"] is matcher._rhs_config


def test_get_join_accepts_enum_members_and_strings():
    """Enum members and their string values resolve to the same join."""
    assert get_join(LhsKind.USRN, GeometryType.POINT) is get_join("usrn", "point")


def test_registry_contains_expected_modes():
    """The join registry exposes USRN's polygon, point and line modes, plus UPRN's polygon mode."""
    assert (LhsKind.USRN, GeometryType.POLYGON) in _registry
    assert (LhsKind.USRN, GeometryType.POINT) in _registry
    assert (LhsKind.USRN, GeometryType.LINE) in _registry
    assert (LhsKind.UPRN, GeometryType.POLYGON) in _registry
    assert all(isinstance(get_join(*k), JoinFn) for k in _registry)


def test_filtered_mode_rejects_oversized_bbox():
    """FilteredMode rejects a bbox above the area limit."""
    # 200km × 200km = 40,000 km² — well above the 3,000 km² limit
    with pytest.raises(ValueError, match="km².*limit"):
        FilteredMode(bbox=(0, 0, 200_000, 200_000))


def test_filtered_mode_accepts_city_sized_bbox():
    """FilteredMode accepts a city-sized bbox under the area limit."""
    # London bbox — should be just under the limit
    FilteredMode(bbox=(503000, 156000, 562000, 201000))
