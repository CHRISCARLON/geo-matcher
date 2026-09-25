"""Unit tests for geo_matcher/join_sql.py — SQL fragment builders."""

import pytest

from geo_matcher.join_sql import (
    _bbox_to_wkt,
    bbox_nearest_filters,
    fill_spatial_filter,
    sql_ident,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# _bbox_to_wkt
# ---------------------------------------------------------------------------


def test_bbox_to_wkt_exact_polygon():
    """A bbox with no expansion produces the exact closed-ring WKT."""
    assert _bbox_to_wkt([100.0, 200.0, 300.0, 400.0]) == (
        "POLYGON((100.0 200.0,300.0 200.0,300.0 400.0,100.0 400.0,100.0 200.0))"
    )


def test_bbox_to_wkt_expand_m_grows_every_side():
    """expand_m grows xmin/ymin inward (down) and xmax/ymax outward (up) by the same amount."""
    wkt = _bbox_to_wkt([100.0, 200.0, 300.0, 400.0], expand_m=10.0)
    assert wkt == "POLYGON((90.0 190.0,310.0 190.0,310.0 410.0,90.0 410.0,90.0 190.0))"


# ---------------------------------------------------------------------------
# bbox_nearest_filters
# ---------------------------------------------------------------------------


def test_bbox_nearest_filters_u_uses_exact_bbox_s_is_expanded():
    """u is pruned to the exact bbox; s is expanded by distance_m so nearby features aren't missed."""
    clause = bbox_nearest_filters([0.0, 0.0, 100.0, 100.0], distance_m=10.0)
    u_clause, s_clause = clause.split(" AND ")
    u_wkt = u_clause.split("ST_GeomFromWKT('")[1].split("')")[0]
    s_wkt = s_clause.split("ST_GeomFromWKT('")[1].split("')")[0]
    assert u_wkt == _bbox_to_wkt([0.0, 0.0, 100.0, 100.0])
    assert s_wkt == _bbox_to_wkt([0.0, 0.0, 100.0, 100.0], expand_m=10.0)


# ---------------------------------------------------------------------------
# sql_ident
# ---------------------------------------------------------------------------


def test_sql_ident_rejects_embedded_double_quote():
    """A double quote would let the value break out of the quoted SQL identifier."""
    with pytest.raises(ValueError, match="double-quote"):
        sql_ident('foo" ; DROP TABLE usrns; --')


# ---------------------------------------------------------------------------
# fill_spatial_filter
# ---------------------------------------------------------------------------


def test_fill_spatial_filter_raises_on_leftover_brace():
    """A stray, unrecognised {brace} left in the template after formatting is caught here
    rather than reaching Sedona/DuckDB as a confusing SQL parse error."""
    template = "SELECT 1 {spatial_filter} {oops}"
    with pytest.raises(ValueError, match="unfilled placeholder"):
        fill_spatial_filter(template, spatial_filter="AND a", oops="{still_a_brace}")
