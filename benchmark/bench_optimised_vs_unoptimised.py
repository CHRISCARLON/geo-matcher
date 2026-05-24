"""Benchmark USRN Parquet variants with DuckDB and SedonaDB.

  Optimised  : uses bbox STRUCT predicate → both engines can prune row groups
  Unoptimised: falls back to ST_Intersects → full scan required

Usage:
    python bench_optimised_vs_unoptimised.py
    python bench_optimised_vs_unoptimised.py \\
        --variant optimised   output_data/usrns_27700.parquet \\
        --variant unoptimised output_data/usrns_unoptimised.parquet
"""

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any

import duckdb
import sedona.db

from usrn_matcher.bboxes import LEEDS
from usrn_matcher.explain import (
    _build_plan_tree,
)


@dataclass(frozen=True)
class Variant:
    label: str
    path: str


@dataclass(frozen=True)
class DuckResult:
    strategy: str
    filter_text: str
    scanned_rows: int | None
    elapsed: float
    output_rows: int


@dataclass(frozen=True)
class SedonaResult:
    strategy: str
    pruning: dict[str, tuple[int, int]]
    scanned_rows: int | None
    elapsed: float
    output_rows: int


CITIES: dict[str, list[int]] = {"Leeds": LEEDS}

parser = argparse.ArgumentParser(description="Benchmark USRN Parquet variants.")
parser.add_argument(
    "--variant",
    nargs=2,
    metavar=("LABEL", "USRN_PARQUET"),
    action="append",
)
args = parser.parse_args()

_DEFAULT_VARIANTS = [
    ("optimised", "output_data/usrns_27700.parquet"),
    ("unoptimised", "output_data/usrns_unoptimised.parquet"),
]

variants: list[Variant] = [
    Variant(label=label, path=path)
    for label, path in (args.variant or _DEFAULT_VARIANTS)
]


duck = duckdb.connect()
duck.execute("LOAD spatial;")

sd = sedona.db.connect()


_schema_cache: dict[str, set[str]] = {}
_rg_cache: dict[str, int] = {}
_row_cache: dict[str, int] = {}
_BOX_CHARS = re.compile(r"[┌┐└┘├┤┬┴┼─│╭╮╯╰]+")
_PRUNING_METRICS = ("row_groups_pruned_statistics", "row_groups_spatial_pruned")
_RG_STAT_RE = re.compile(r"(\d+) total.*?(\d+) matched")  # "N total → M matched"


def _schema_cols(path: str) -> set[str]:
    if path not in _schema_cache:
        rows = duck.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0"
        ).fetchall()
        _schema_cache[path] = {row[0] for row in rows}
    return _schema_cache[path]


def _has_bbox_struct(path: str) -> bool:
    return "bbox" in _schema_cols(path)


def _total_row_groups(path: str) -> int:
    if path not in _rg_cache:
        row = duck.sql(
            f"SELECT num_row_groups FROM parquet_file_metadata('{path}')"
        ).fetchone()
        assert row is not None, f"parquet_file_metadata returned no rows for {path!r}"
        _rg_cache[path] = row[0]
    return _rg_cache[path]


def _total_rows(path: str) -> int:
    if path not in _row_cache:
        row = duck.sql(
            f"SELECT num_rows FROM parquet_file_metadata('{path}')"
        ).fetchone()
        assert row is not None, f"parquet_file_metadata returned no rows for {path!r}"
        _row_cache[path] = row[0]
    return _row_cache[path]


def _sedona_query(path: str, bbox: list[int]) -> str:
    xmin, ymin, xmax, ymax = bbox
    wkt = f"POLYGON(({xmin} {ymin},{xmax} {ymin},{xmax} {ymax},{xmin} {ymax},{xmin} {ymin}))"
    return f"SELECT * FROM usrns WHERE ST_Intersects(geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 27700))"


def _extract_plan_filters(explain_text: str) -> str:
    collecting = False
    filters: list[str] = []
    for line in explain_text.splitlines():
        clean = _BOX_CHARS.sub("", line).strip()
        if not clean:
            continue
        if "Filters:" in clean:
            collecting = True
            continue
        if collecting:
            if any(kw in clean for kw in ("rows", "Files Read", "Filename")):
                break
            filters.append(clean)
    return " ".join(filters) if filters else "(no filter extracted)"


def _duck_scan_rows(path: str, bbox: list[int], force_spatial: bool) -> int | None:
    """Estimate rows DuckDB physically reads from parquet (before predicate evaluation).

    ST_Intersects: no row group pruning → scanned = entire file.
    bbox struct:   DuckDB prunes via column statistics on bbox.* sub-fields.
    """
    total = _total_rows(path)
    if force_spatial or not _has_bbox_struct(path):
        return total
    xmin, ymin, xmax, ymax = bbox
    rows_per_rg = total / _total_row_groups(path)
    try:
        row = duck.sql(f"""
            WITH
            xmin_ok AS (
                SELECT row_group_id FROM parquet_metadata('{path}')
                WHERE path_in_schema = 'bbox, xmin'
                  AND TRY_CAST(stats_min_value AS DOUBLE) <= {xmax}
            ),
            xmax_ok AS (
                SELECT row_group_id FROM parquet_metadata('{path}')
                WHERE path_in_schema = 'bbox, xmax'
                  AND TRY_CAST(stats_max_value AS DOUBLE) >= {xmin}
            ),
            ymin_ok AS (
                SELECT row_group_id FROM parquet_metadata('{path}')
                WHERE path_in_schema = 'bbox, ymin'
                  AND TRY_CAST(stats_min_value AS DOUBLE) <= {ymax}
            ),
            ymax_ok AS (
                SELECT row_group_id FROM parquet_metadata('{path}')
                WHERE path_in_schema = 'bbox, ymax'
                  AND TRY_CAST(stats_max_value AS DOUBLE) >= {ymin}
            )
            SELECT COUNT(*) FROM xmin_ok
            JOIN xmax_ok USING (row_group_id)
            JOIN ymin_ok USING (row_group_id)
            JOIN ymax_ok USING (row_group_id)
        """).fetchone()
        assert row is not None
        return round(row[0] * rows_per_rg)
    except Exception:
        return None


def _parse_rg_stat(val: Any) -> tuple[int, int] | None:
    """Return (total, matched) from 'N total → M matched', or None."""
    if isinstance(val, int):
        return (val, 0)
    m = _RG_STAT_RE.search(str(val))
    if m:
        return int(m.group(1)), int(m.group(2))
    return None


def _collect_pruning(node: dict[str, Any], acc: dict[str, tuple[int, int]]) -> None:
    for key in _PRUNING_METRICS:
        if key in node:
            parsed = _parse_rg_stat(node[key])
            if parsed is not None:
                prev = acc.get(key, (0, 0))
                acc[key] = (prev[0] + parsed[0], prev[1] + parsed[1])
    for child in node.get("children", []):
        _collect_pruning(child, acc)


def _find_datasource_rows(node: dict[str, Any]) -> int | None:
    if "DataSource" in node.get("node", "") and "output_rows" in node:
        return int(node["output_rows"])
    for child in node.get("children", []):
        result = _find_datasource_rows(child)
        if result is not None:
            return result
    return None


def _sedona_pruning_stats(
    sql: str, file_row_groups: int
) -> tuple[dict[str, tuple[int, int]], int | None]:
    tbl = sd.sql(f"EXPLAIN ANALYZE {sql}").to_arrow_table()
    plan_types = tbl.column("plan_type").to_pylist()
    plans = tbl.column("plan").to_pylist()
    text = next(
        (p for pt, p in zip(plan_types, plans) if "Metrics" in pt),
        plans[0] if plans else "",
    )
    tree = _build_plan_tree(text)
    acc: dict[str, tuple[int, int]] = {}
    _collect_pruning(tree, acc)
    rows_scanned = _find_datasource_rows(tree)

    # Normalise raw totals (row_groups × parallelism) back to actual row groups.
    normalised: dict[str, tuple[int, int]] = {}
    for key, (raw_total, raw_matched) in acc.items():
        if raw_total > 0:
            factor = raw_total / file_row_groups
            normalised[key] = (file_row_groups, round(raw_matched / factor))
        else:
            normalised[key] = (raw_total, raw_matched)

    return normalised, rows_scanned


# ── DuckDB runner ──────────────────────────────────────────────────────────────


def _run_duck(path: str, bbox: list[int], force_spatial: bool = False) -> DuckResult:
    xmin, ymin, xmax, ymax = bbox
    if _has_bbox_struct(path) and not force_spatial:
        predicate = (
            f"bbox.xmin <= {xmax} AND bbox.xmax >= {xmin} "
            f"AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin}"
        )
        strategy = "bbox struct"
    else:
        predicate = (
            f"ST_Intersects(geometry, ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
        )
        strategy = "ST_Intersects"
    sql = f"SELECT * FROM read_parquet('{path}') WHERE {predicate}"
    explain_text = "\n".join(
        row[1] for row in duck.sql(f"EXPLAIN ANALYZE {sql}").fetchall()
    )
    scan_rows = _duck_scan_rows(path, bbox, force_spatial)
    t0 = time.perf_counter()
    output_rows = len(duck.sql(sql).fetchall())
    return DuckResult(
        strategy=strategy,
        filter_text=_extract_plan_filters(explain_text),
        scanned_rows=scan_rows,
        elapsed=time.perf_counter() - t0,
        output_rows=output_rows,
    )


# ── SedonaDB runner ────────────────────────────────────────────────────────────


def _run_sedona(path: str, bbox: list[int]) -> SedonaResult:
    sd.read_parquet(path).to_view("usrns", overwrite=True)
    sql = _sedona_query(path, bbox)
    pruning, rows_scanned = _sedona_pruning_stats(sql, _total_row_groups(path))
    t0 = time.perf_counter()
    result = sd.sql(sql).to_arrow_table()
    return SedonaResult(
        strategy="ST_Intersects",
        pruning=pruning,
        scanned_rows=rows_scanned,
        elapsed=time.perf_counter() - t0,
        output_rows=len(result),
    )


# ── Main loop ──────────────────────────────────────────────────────────────────

for city, bbox in [("Leeds", LEEDS)]:
    print(f"\n{'=' * 60}")
    print(f"  {city}  bbox={bbox}")
    print(f"{'=' * 60}")

    for v in variants:
        total_rgs = _total_row_groups(v.path)
        print(f"\n  [{v.label}]  row_groups={total_rgs}")

        file_rows = _total_rows(v.path)
        duck_r = _run_duck(v.path, bbox)
        scan_str = (
            f"{duck_r.scanned_rows:,}" if duck_r.scanned_rows is not None else "?"
        )
        print(
            f"    DuckDB   ({duck_r.strategy:12s})  {duck_r.elapsed:.3f}s"
            f"  out={duck_r.output_rows:,} / scanned={scan_str} / file={file_rows:,} rows"
        )
        print(f"      filter: {duck_r.filter_text}")

        if _has_bbox_struct(v.path):
            sp = _run_duck(v.path, bbox, force_spatial=True)
            sp_scan_str = f"{sp.scanned_rows:,}" if sp.scanned_rows is not None else "?"
            print(
                f"    DuckDB   (ST_Intersects)  {sp.elapsed:.3f}s"
                f"  out={sp.output_rows:,} / scanned={sp_scan_str} / file={file_rows:,} rows  ← no pruning"
            )
            print(f"      filter: {sp.filter_text}")

        try:
            sd_r = _run_sedona(v.path, bbox)
            pruning_str = (
                "  ".join(
                    f"{k}={matched}/{total} rgs"
                    for k, (total, matched) in sd_r.pruning.items()
                )
                or "none"
            )
            scanned_str = (
                f"{sd_r.scanned_rows:,}" if sd_r.scanned_rows is not None else "?"
            )
            print(
                f"    Sedona   ({sd_r.strategy:12s})  {sd_r.elapsed:.3f}s"
                f"  out={sd_r.output_rows:,} / scanned={scanned_str} / file={file_rows:,} rows"
            )
            print(f"      pruning: {pruning_str}")
        except Exception as exc:
            print(f"    Sedona   ERROR: {exc}")
