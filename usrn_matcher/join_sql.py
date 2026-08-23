import functools
import logging
import re
from collections.abc import Sequence

import pyarrow.parquet as pq

from .config import BBox, DatasetConfig
from .logger import get_logger

log: logging.Logger = get_logger()

# EPSG:27700 (British National Grid) — every prepared file and every filter
# fragment in this pipeline is in this CRS. One constant instead of a literal
# repeated in every WKT-builder below.
_SRID: int = 27700

_UNSAFE_IDENT_PATTERN = re.compile(r'["\x00-\x1f\x7f]')


def sql_ident(name: str, *, context: str = "identifier") -> str:
    """Validate *name* is safe to embed inside a double-quoted SQL identifier.

    Rejects double quotes (would let the value break out of the quoting) and
    control characters. Deliberately not a strict allowlist — real column
    names routinely contain spaces, parentheses, hyphens, slashes, etc.
    (``test_col_fragment_explicit_columns_with_spaces`` relies on spaces being
    allowed) — so this is a narrow guard against the actual injection vector
    rather than a restrictive charset check.
    """
    if not name:
        raise ValueError(f"SQL {context} must not be empty.")
    if _UNSAFE_IDENT_PATTERN.search(name):
        raise ValueError(
            f"SQL {context} {name!r} contains a double-quote or control "
            "character — not safe to interpolate into a quoted SQL identifier."
        )
    return name


def _bbox_to_wkt(bbox: BBox, expand_m: float = 0.0) -> str:
    """Return a closed POLYGON WKT for *bbox*, grown by *expand_m* metres on every side."""
    xmin, ymin, xmax, ymax = bbox
    x0, y0, x1, y1 = xmin - expand_m, ymin - expand_m, xmax + expand_m, ymax + expand_m
    return f"POLYGON(({x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))"


def usrn_spatial_filter(bbox: BBox, expand_m: float = 0.0, alias: str = "u") -> str:
    """Return the ``AND ST_Intersects(...)`` fragment that prunes a USRN-side scan.

    *alias* selects which relation to prune — ``u`` for the centreline table, ``ul``
    for the buffered corridor table that Phase 1 joins for overlap scoring.
    """
    wkt = _bbox_to_wkt(bbox, expand_m)
    return (
        f"AND ST_Intersects({alias}.geometry, "
        f"ST_SetSRID(ST_GeomFromWKT('{wkt}'), {_SRID}))"
    )


def bbox_pruner(bbox: BBox) -> str:
    """Return AND conditions that prune both parquet scans to the given EPSG:27700 bbox.

    Uses ST_Intersects against a fixed bbox polygon so Sedona can skip row groups
    via GeoParquet 1.1 covering metadata (row_groups_spatial_pruned path).

    Used as ``execute_join``'s ``filter_fn`` for intersect-style joins, and called
    directly by ``_filtered_line_join`` for the Phase 1 prune.
    """
    bbox_geom = f"ST_SetSRID(ST_GeomFromWKT('{_bbox_to_wkt(bbox)}'), {_SRID})"
    return (
        f"AND ST_Intersects(u.geometry, {bbox_geom})"
        f" AND ST_Intersects(s.geometry, {bbox_geom})"
    )


def bbox_nearest_filters(bbox: BBox, distance_m: float) -> str:
    """Return AND conditions that prune both sides of a nearest/line join.

    USRNs (``u``) are filtered to the exact bbox; RHS (``s``) is expanded by
    ``distance_m`` so features just outside the boundary can still match.

    Uses ST_Intersects against fixed bbox polygons so Sedona can skip row groups
    via GeoParquet 1.1 covering metadata (row_groups_spatial_pruned path).
    """

    def _spatial_predicate(alias: str, expand_m: float) -> str:
        wkt = _bbox_to_wkt(bbox, expand_m)
        return f"ST_Intersects({alias}.geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), {_SRID}))"

    return (
        f"AND {_spatial_predicate('u', 0.0)} AND {_spatial_predicate('s', distance_m)}"
    )


def fill_spatial_filter(template: str, **fragments: str) -> str:
    """Fill a query template's ``{spatial_filter}``/``{corridor_filter}`` placeholders.

    Thin wrapper around ``str.format`` so every call site shares one guard: if a
    brace survives the substitution — a typo'd placeholder name, or a fragment
    that was forgotten — this raises here with the fragment names that were
    actually supplied, instead of the failure surfacing as a cryptic DuckDB/
    Sedona SQL parse error once the query reaches the engine.
    """
    filled = template.format(**fragments)
    if "{" in filled or "}" in filled:
        raise ValueError(
            "Query template still contains an unfilled placeholder after "
            f".format(**{sorted(fragments)}) — check the template's {{...}} "
            "names match the fragment names passed in."
        )
    return filled


@functools.lru_cache(maxsize=None)
def _read_auto_cols(parquet_path: str) -> tuple[str, ...]:
    """Return non-geometry column names from a parquet footer — cached per path."""
    schema = pq.read_schema(parquet_path)
    log.debug(f"Right handside dataset schema is: {schema}")
    return tuple(name for name in schema.names if name not in ("geometry", "bbox"))


def col_fragment(rhs_config: DatasetConfig) -> str:
    """Return the SELECT fragment for RHS columns (prefixed with a leading comma).

    If ``rhs_config.columns`` is non-empty the listed columns are used as-is.
    Otherwise all columns except ``geometry`` and the internal ``bbox`` covering
    column are discovered from the parquet file footer. Every column name is
    validated via ``sql_ident`` before being embedded, regardless of source.
    """
    cols: Sequence[str]
    if rhs_config.columns:
        cols = rhs_config.columns
    else:
        # rhs_df.schema is a PySedonaSchema (not iterable like PyArrow) so read
        # column names directly from the parquet file footer instead.
        # Result is cached per path — only one I/O hit per process.
        cols = _read_auto_cols(str(rhs_config.parquet_path))
    return ", " + ", ".join(f's."{sql_ident(c, context="RHS column")}"' for c in cols)


# Overlap scoring for Phases 1 and 2. ``_u_geom`` is the pre-buffered corridor polygon
# from ``usrns_line.geometry`` in both cases, so it is intersected directly.
#
# Phase 1 matches against raw centrelines, so it joins ``usrns_line`` on ``usrn`` purely
# to fetch that corridor. Buffering the centreline per row here instead — which is what
# this used to do — cost 97-99% of Phase 1's wall time (78s of an 82s chunk on
# gas_pipe), because ST_Buffer scales with vertex count. The corridor is already on
# disk; reading it is ~20-57x faster and produces bit-identical scores.
#
# This does couple the score to the corridor file's buffer width rather than to
# ``--distance``: if ``prepare-usrns-line --buffer-m`` differs from the match-time
# ``--distance``, Phase 1's overlap is measured against the file's width. Phase 2 has
# always behaved that way, so the two phases are now consistent.
OVERLAP_EXPR_CORRIDOR = """
    COALESCE(
        ST_Length(
            ST_Intersection(
                ST_GeomFromWKB(_u_geom),
                ST_GeomFromWKB(_s_geom)
            )
        ) / NULLIF(GREATEST(ST_Length(ST_GeomFromWKB(_s_geom)), {denom}), 0),
        0.0
    )
"""
