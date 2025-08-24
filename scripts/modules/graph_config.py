#!/usr/bin/env python3
"""
graph_config.py
----------------
Parse, validate, and expose **graph generation settings** as attributes used by
graph workers. This module:

1) Resolves all important directories from the unified paths block via
   `utils.resolve_all_paths(settings)`.
2) Reads canvas size and max_hops directly from settings with robust fallbacks.
3) Exposes the HTML time ranges used to pick JSON bundles.
4) Parses graph-generation knobs (parallelism, niceness, cadence, etc.).

It purposefully **does not** import or unpack `utils.resolve_canvas()` anymore.
Your current utils version returns a dict (html_dir/graph_dir/time_ranges), not
(width, height, max_hops); unpacking that was the cause of:
`invalid literal for int() with base 10: 'html_dir'`.

This file is suitable for users with basic Python knowledge. Each section is
documented inline.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from modules.utils import resolve_all_paths, get_html_ranges


def _to_int(value: Any, default: int) -> int:
    """
    Best-effort integer conversion with a safe default.

    Accepts numbers or numeric strings. Returns `default` for None/empty/invalid.
    """
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            # Avoid True -> 1, False -> 0 surprises for config booleans
            return default
        return int(str(value).strip())
    except Exception:
        return default


class GraphConfig:
    """
    Container of all graph-related configuration.

    Attributes exposed (most relevant):
      - RRD_DIR:     directory that holds per-IP RRDs
      - GRAPH_DIR:   directory where PNG/SVG graphs are written
      - TRACE_DIR:   directory that contains traceroute artifacts
      - WIDTH:       graph canvas width (pixels)
      - HEIGHT:      graph canvas height (pixels)
      - MAX_HOPS:    maximum hops plotted on overlay graphs
      - TIME_RANGES: list of dicts: [{'label': '1h', 'seconds': 3600}, ...]
      - DATA_SOURCES:list of DS names to include on overlay graphs
      - EXECUTOR_KIND: "process" | "thread"
      - SKIP_UNCHANGED: skip jobs with unchanged inputs
      - RECENT_SAFETY_SECONDS: ignore very recent timestamps to avoid races
      - NICENESS:     os.nice() value applied at load time (best effort)
      - CPU_AFFINITY: "none" | "spread" (hint for worker placement)
      - USE_RRD_LOCK: if True, serialize RRD reads with a lock
      - SUMMARY_EVERY: print a progress summary every N runs
      - HOPS_EVERY:    recompute dynamic hop set every N runs
      - PARALLELISM:   worker parallelism; "auto" maps to os.cpu_count()
      - STATE_PATH:    path to internal cadence state file
    """

    def __init__(self, settings: Dict[str, Any]) -> None:
        # --- 1) Resolve unified directories ---------------------------------
        paths = resolve_all_paths(settings)
        self.RRD_DIR: str = paths["rrd"]
        self.GRAPH_DIR: str = paths["graphs"]
        self.TRACE_DIR: str = paths["traceroute"]

        # --- 2) Canvas + hops with legacy fallbacks --------------------------
        # Preferred block: settings['graph_canvas'] = {width,height,max_hops}
        canvas = (settings or {}).get("graph_canvas", {}) or {}

        # Legacy fallbacks: graph_width / graph_height / max_hops at root
        self.WIDTH: int = _to_int(canvas.get("width", settings.get("graph_width")), 800)
        self.HEIGHT: int = _to_int(canvas.get("height", settings.get("graph_height")), 200)
        self.MAX_HOPS: int = _to_int(canvas.get("max_hops", settings.get("max_hops")), 30)

        # --- 3) HTML time ranges (with legacy fallback inside util) ----------
        # Produces: [{'label': '1h', 'seconds': 3600}, ...]
        self.TIME_RANGES: List[Dict[str, Any]] = get_html_ranges(settings)

        # --- 4) DS names used for overlay graphs -----------------------------
        # Reads from settings['rrd']['data_sources'], tolerates missing keys.
        ds_block = (settings or {}).get("rrd", {}).get("data_sources", []) or []
        self.DATA_SOURCES: List[str] = [ds["name"] for ds in ds_block if isinstance(ds, dict) and ds.get("name")]

        # --- 5) Graph generation knobs ---------------------------------------
        gg = (settings or {}).get("graph_generation", {}) or {}

        # Executor kind: "process" (default) or "thread"
        self.EXECUTOR_KIND: str = str(gg.get("executor", "process")).lower()

        # Skip jobs when inputs unchanged since last run
        self.SKIP_UNCHANGED: bool = bool(gg.get("skip_unchanged", True))

        # Ignore very recent timestamps to avoid partial reads
        self.RECENT_SAFETY_SECONDS: int = _to_int(gg.get("recent_safety_seconds", 120), 120)

        # Hint to OS scheduler; failing os.nice() is OK
        self.NICENESS: int = _to_int(gg.get("niceness", 5), 5)

        # Affinity mode (hint only): "none" or "spread"
        self.CPU_AFFINITY: str = str(gg.get("cpu_affinity", "none")).lower()

        # Serialize RRD reads if backend/filesystem benefits from it
        self.USE_RRD_LOCK: bool = bool(gg.get("use_rrd_lock", True))

        # Cadence counters for features that need less-frequent refresh
        self.SUMMARY_EVERY: int = _to_int(gg.get("summary_interval_runs", 1), 1)
        self.HOPS_EVERY: int = _to_int(gg.get("hop_interval_runs", 5), 5)

        # Parallelism supports numeric values or the string "auto"
        raw_par = gg.get("parallelism", 4)
        if isinstance(raw_par, str):
            v = raw_par.strip().lower()
            if v == "auto":
                self.PARALLELISM: int = os.cpu_count() or 2
            else:
                self.PARALLELISM = _to_int(v, 4)
        else:
            self.PARALLELISM = _to_int(raw_par, 4)

        # Where we store internal state (e.g., run counters)
        self.STATE_PATH: str = os.path.join(self.GRAPH_DIR, ".graph_state.json")


def load_graph_config(settings: Dict[str, Any]) -> GraphConfig:
    """
    Factory that applies niceness (best effort) after building GraphConfig.

    Returns:
        GraphConfig: a fully-populated configuration object.
    """
    cfg = GraphConfig(settings)
    try:
        if cfg.NICENESS:
            os.nice(cfg.NICENESS)
    except Exception:
        # Not fatal on platforms/users where nice(2) is disallowed.
        pass
    return cfg
