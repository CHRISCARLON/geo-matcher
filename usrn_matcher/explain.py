"""Explain-plan utilities for Sedona spatial joins.

Parses the indented text from ``EXPLAIN ANALYZE`` into a nested dict tree and
logs it as coloured JSON — one metric per line, top-to-bottom.

Note on ``row_groups_spatial_pruned`` counts
--------------------------------------------
Sedona reports row-group pruning evaluations *across all parallel workers*, not
unique row groups in the file.  With N workers, the total = file_row_groups × N
and matched = matched_per_worker × N, so the pruning ratio is correct but the
absolute numbers are inflated by parallelism.
"""

import json
import re
import sys
from typing import Any

from sedonadb.context import SedonaContext

from .logger import get_logger

log = get_logger()

_COLOUR = sys.stderr.isatty()
_CYAN = "\033[36m" if _COLOUR else ""
_GREEN = "\033[32m" if _COLOUR else ""
_BLUE = "\033[34m" if _COLOUR else ""
_RESET = "\033[0m" if _COLOUR else ""
_RULE = f"{_BLUE}{'─' * 60}{_RESET}"

_METRICS_BLOCK_RE = re.compile(r",?\s*metrics=\[([^\]]*)\]")
_KV_RE = re.compile(r"(\w+)=([^,\]]+)")
_KEEP_METRICS = frozenset({
    "output_rows", "elapsed_compute", "bytes_scanned", "output_bytes",
    "row_groups_spatial_pruned", "row_groups_pruned_statistics",
    "files_ranges_spatial_pruned", "selectivity",
    "build_mem_used", "join_time", "execution_mode",
    "fetch_time", "time_elapsed_scanning_total", "metadata_load_time",
})
_MAX_PARAMS = 100  # truncate verbose params (file_groups, projection lists, etc.)

_NODE_JSON_RE = re.compile(r'("node":\s*)"([^"]+)"')
_METRIC_JSON_RE = re.compile(
    r'"(' + "|".join(sorted(_KEEP_METRICS)) + r')":\s*(\d+|"[^"]+")'
)


def _parse_plan_node(stripped: str) -> dict[str, Any]:
    """Parse one indented plan line into a node dict (no children yet)."""
    m = _METRICS_BLOCK_RE.search(stripped)
    metrics_text = m.group(1) if m else ""
    header = _METRICS_BLOCK_RE.sub("", stripped).rstrip(", ") if m else stripped

    node_name, _, params = header.partition(":")
    d: dict[str, Any] = {"node": node_name.strip()}
    if params.strip():
        p = params.strip()
        d["params"] = (p[:_MAX_PARAMS] + "…") if len(p) > _MAX_PARAMS else p

    for k, v in _KV_RE.findall(metrics_text):
        if k in _KEEP_METRICS:
            val = v.strip()
            try:
                d[k] = int(val)
            except ValueError:
                d[k] = val

    return d


def _build_plan_tree(text: str) -> dict[str, Any]:
    """Parse indented plan text into a nested dict tree."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return {}

    stack: list[tuple[int, dict]] = []
    root: dict[str, Any] = {}

    for line in lines:
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        node = _parse_plan_node(stripped)

        while stack and stack[-1][0] >= indent:
            stack.pop()

        if stack:
            stack[-1][1].setdefault("children", []).append(node)
        else:
            root = node

        stack.append((indent, node))

    return root


def _colour_json(text: str) -> str:
    text = _NODE_JSON_RE.sub(
        lambda m: f'{m.group(1)}{_CYAN}"{m.group(2)}"{_RESET}', text
    )
    text = _METRIC_JSON_RE.sub(
        lambda m: f'"{m.group(1)}": {_GREEN}{m.group(2)}{_RESET}', text
    )
    return text


def log_plan(sd: SedonaContext, query: str) -> None:
    """Run ``EXPLAIN ANALYZE`` and log the plan as a coloured JSON tree."""
    tbl = sd.sql(f"EXPLAIN ANALYZE {query}").to_arrow_table()
    plan_types = tbl.column("plan_type").to_pylist()
    plans = tbl.column("plan").to_pylist()
    text = next(
        (p for pt, p in zip(plan_types, plans) if "Metrics" in pt),
        plans[0] if plans else "",
    )
    tree = _build_plan_tree(text)
    plan_json = _colour_json(json.dumps(tree, indent=2, ensure_ascii=False))
    log.info("%s\n%s\n%s", _RULE, plan_json, _RULE)
