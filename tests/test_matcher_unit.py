"""Unit tests for UsrnMatcher dispatch methods"""

import pyarrow as pa
import pytest

from usrn_matcher import DatasetConfig, UsrnMatcher
from usrn_matcher.join import JoinFn, _registry, get_join


@pytest.fixture()
def matcher(tmp_path) -> UsrnMatcher:
    cfg = DatasetConfig(
        name="test",
        source_path=tmp_path / "test.parquet",
        parquet_path=tmp_path / "test.parquet",
    )
    return UsrnMatcher(
        usrn_parquet=tmp_path / "usrns_27700.parquet",
        rhs_config=cfg,
    )


def test_match_dispatch_unknown_mode_raises(matcher):
    with pytest.raises(ValueError, match="Unknown join mode"):
        matcher.match_dispatch(mode="fuzzy")


def test_file_dispatch_unknown_format_raises(matcher, tmp_path):
    with pytest.raises(ValueError, match="Unknown output format"):
        matcher.file_dispatch(pa.table({}), "xlsx", tmp_path, "stem")


def test_join_fns_registry_keys():
    assert "intersect" in _registry
    assert "nearest" in _registry


def test_join_fns_values_are_callable():
    for fn in _registry.values():
        assert callable(fn)


def test_output_formats_registry_keys():
    assert "parquet" in UsrnMatcher._OUTPUT_FORMATS
    assert "csv" in UsrnMatcher._OUTPUT_FORMATS
    assert "sample" in UsrnMatcher._OUTPUT_FORMATS


# ---------------------------------------------------------------------------
# JoinFn runtime checks
# ---------------------------------------------------------------------------


def test_builtin_fns_are_joinfn_instances():
    assert isinstance(get_join("intersect"), JoinFn)
    assert isinstance(get_join("nearest"), JoinFn)


def test_non_callable_is_not_joinfn():
    assert not isinstance("not_a_fn", JoinFn)
    assert not isinstance(42, JoinFn)
