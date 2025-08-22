#!/usr/bin/env python3
"""
Parse, validate, and expose graph generation settings as attributes.

Now reads unified paths from settings['paths'] via utils.resolve_all_paths(),
and canvas size from graph_canvas (with legacy fallbacks).
"""

import os
from modules.utils import resolve_all_paths, resolve_canvas, get_html_ranges  # <- NEW

class GraphConfig:
    def __init__(self, settings):
        # Unified directories (NEW)
        paths = resolve_all_paths(settings)
        self.RRD_DIR     = paths["rrd"]            # was: settings.get("rrd_directory", "data")
        self.GRAPH_DIR   = paths["graphs"]         # was: settings.get("graph_output_directory", "html/graphs")
        self.TRACE_DIR   = paths["traceroute"]     # was: settings.get("traceroute_directory", "traceroute")

        # Canvas & hops (NEW via helper with legacy fallback)
        self.WIDTH, self.HEIGHT, self.MAX_HOPS = resolve_canvas(settings)

        # HTML graph time ranges (NEW helper; falls back to legacy graph_time_ranges)
        self.TIME_RANGES = get_html_ranges(settings)

        # DS names used for overlay graphs (unchanged)
        self.DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]

        # Graph generation knobs
        g = settings.get("graph_generation", {})
        self.EXECUTOR_KIND       = str(g.get("executor", "process")).lower()   # process|thread
        self.SKIP_UNCHANGED      = bool(g.get("skip_unchanged", True))
        self.RECENT_SAFETY_SECONDS = int(g.get("recent_safety_seconds", 120))
        self.NICENESS            = int(g.get("niceness", 5))
        self.CPU_AFFINITY        = str(g.get("cpu_affinity", "none")).lower()  # none|spread
        self.USE_RRD_LOCK        = bool(g.get("use_rrd_lock", True))

        # Cadence
        self.SUMMARY_EVERY = int(g.get("summary_interval_runs", 1))
        self.HOPS_EVERY    = int(g.get("hop_interval_runs", 5))

        # Parallelism: supports "auto"
        raw_par = g.get("parallelism", 2)
        if isinstance(raw_par, str):
            v = raw_par.strip().lower()
            if v == "auto":
                self.PARALLELISM = os.cpu_count() or 2
            else:
                try: self.PARALLELISM = int(v)
                except ValueError: self.PARALLELISM = 2
        else:
            try: self.PARALLELISM = int(raw_par)
            except Exception: self.PARALLELISM = 2

        # State file for cadence counter
        self.STATE_PATH = os.path.join(self.GRAPH_DIR, ".graph_state.json")

def load_graph_config(settings) -> GraphConfig:
    cfg = GraphConfig(settings)
    # niceness (best‑effort)
    try:
        if cfg.NICENESS:
            os.nice(cfg.NICENESS)
    except Exception:
        pass
    return cfg
