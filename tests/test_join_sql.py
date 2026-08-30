"""Unit tests for geo_matcher/join_sql.py — SQL fragment builders."""

import pytest

from geo_matcher.join_sql import (
    OVERLAP_EXPR_CORRIDOR,
    _bbox_to_wkt,
    bbox_nearest_filters,
    bbox_pruner,
    fill_spatial_filter,
    sql_ident,
    usrn_spatial_filter,
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
# usrn_spatial_filter
# ---------------------------------------------------------------------------


def test_usrn_spatial_filter_default_alias_is_u():
    clause = usrn_spatial_filter([0.0, 0.0, 100.0, 100.0])
    assert clause.startswith("AND ST_Intersects(u.geometry,")
    assert "27700" in clause


def test_usrn_spatial_filter_custom_alias():
    """alias='ul' targets the buffered corridor table Phase 1 joins for overlap scoring."""
    clause = usrn_spatial_filter([0.0, 0.0, 100.0, 100.0], alias="ul")
    assert clause.startswith("AND ST_Intersects(ul.geometry,")


def test_usrn_spatial_filter_expand_m_widens_the_wkt():
    tight = usrn_spatial_filter([0.0, 0.0, 100.0, 100.0])
    wide = usrn_spatial_filter([0.0, 0.0, 100.0, 100.0], expand_m=10.0)
    assert tight != wide
    assert "-10.0" in wide  # xmin/ymin shifted outward by expand_m
    assert "-10.0" not in tight


# ---------------------------------------------------------------------------
# bbox_pruner
# ---------------------------------------------------------------------------


def test_bbox_pruner_produces_where_clause():
    """bbox_pruner builds an ST_Intersects clause for both sides."""
    clause = bbox_pruner([100.0, 200.0, 300.0, 400.0])
    assert "ST_Intersects(u.geometry" in clause
    assert "ST_Intersects(s.geometry" in clause
    assert "100.0" in clause
    assert "400.0" in clause


def test_bbox_pruner_uses_same_bbox_geom_for_both_sides():
    """u and s share one un-expanded bbox polygon — the exact same WKT literal twice."""
    clause = bbox_pruner([0.0, 0.0, 100.0, 100.0])
    bbox_geom = clause.split("AND ST_Intersects(u.geometry, ")[1].split(")\n")[0]
    assert (
        clause.count(bbox_geom.split(")")[0]) >= 1
    )  # sanity: fragment actually present
    # u's bbox literal and s's bbox literal are identical (no per-side expansion here)
    u_clause, s_clause = clause.split(" AND ")
    u_wkt = u_clause.split("ST_GeomFromWKT('")[1].split("')")[0]
    s_wkt = s_clause.split("ST_GeomFromWKT('")[1].split("')")[0]
    assert u_wkt == s_wkt


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


def test_bbox_nearest_filters_zero_distance_matches_bbox_pruner_shape():
    """distance_m=0 collapses s's expansion to nothing — both sides use the exact bbox."""
    clause = bbox_nearest_filters([0.0, 0.0, 100.0, 100.0], distance_m=0.0)
    u_clause, s_clause = clause.split(" AND ")
    u_wkt = u_clause.split("ST_GeomFromWKT('")[1].split("')")[0]
    s_wkt = s_clause.split("ST_GeomFromWKT('")[1].split("')")[0]
    assert u_wkt == s_wkt


# ---------------------------------------------------------------------------
# sql_ident
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["ATCOCode", "CommonName", "road class", "speed limit", "MAP_SYMBOL", "_id"]
)
def test_sql_ident_accepts_ordinary_names(name):
    """Ordinary identifiers — including ones with spaces — pass through unchanged."""
    assert sql_ident(name) == name


def test_sql_ident_rejects_embedded_double_quote():
    """A double quote would let the value break out of the quoted SQL identifier."""
    with pytest.raises(ValueError, match="double-quote"):
        sql_ident('foo" ; DROP TABLE usrns; --')


@pytest.mark.parametrize("bad", ["a\nb", "a\tb", "a\x00b"])
def test_sql_ident_rejects_control_characters(bad):
    with pytest.raises(ValueError, match="control"):
        sql_ident(bad)


def test_sql_ident_rejects_empty_string():
    with pytest.raises(ValueError, match="empty"):
        sql_ident("")


def test_sql_ident_error_message_includes_context():
    """The error names which field failed, e.g. '--rhs-id-col', so a CLI user can act on it."""
    with pytest.raises(ValueError, match="--rhs-id-col"):
        sql_ident('bad"col', context="--rhs-id-col")


# ---------------------------------------------------------------------------
# fill_spatial_filter
# ---------------------------------------------------------------------------


def test_fill_spatial_filter_substitutes_placeholder():
    template = "SELECT 1 WHERE TRUE {spatial_filter}"
    assert fill_spatial_filter(template, spatial_filter="AND x") == (
        "SELECT 1 WHERE TRUE AND x"
    )


def test_fill_spatial_filter_substitutes_multiple_placeholders():
    template = "SELECT 1 {spatial_filter} {corridor_filter}"
    filled = fill_spatial_filter(
        template, spatial_filter="AND a", corridor_filter="AND b"
    )
    assert filled == "SELECT 1 AND a AND b"


def test_fill_spatial_filter_raises_on_missing_fragment():
    """A template placeholder with no matching fragment is a KeyError from str.format —
    surfaced immediately rather than producing a query with a literal unfilled brace."""
    template = "SELECT 1 {spatial_filter} {corridor_filter}"
    with pytest.raises(KeyError):
        fill_spatial_filter(template, spatial_filter="AND a")


def test_fill_spatial_filter_raises_on_leftover_brace():
    """A stray, unrecognised {brace} left in the template after formatting is caught here
    rather than reaching Sedona/DuckDB as a confusing SQL parse error."""
    template = "SELECT 1 {spatial_filter} {oops}"
    with pytest.raises(ValueError, match="unfilled placeholder"):
        fill_spatial_filter(template, spatial_filter="AND a", oops="{still_a_brace}")


# ---------------------------------------------------------------------------
# OVERLAP_EXPR_CORRIDOR
# ---------------------------------------------------------------------------


def test_overlap_expr_corridor_denominator_is_filled():
    expr = OVERLAP_EXPR_CORRIDOR.format(denom=20.0)
    assert "{denom}" not in expr
    assert "20.0" in expr


def test_overlap_expr_corridor_zero_distance_still_formats():
    """distance_m=0 → denom=0 still fills cleanly; NULLIF(...,0) in the expression (not
    this .format() call) is what guards the runtime division-by-zero case."""
    expr = OVERLAP_EXPR_CORRIDOR.format(denom=2 * 0.0)
    assert "GREATEST(ST_Length(ST_GeomFromWKB(_s_geom)), 0.0)" in expr
