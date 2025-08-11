#!/usr/bin/env python3

"""
Parse, validate, and expose graph generation settings as attributes.
Keeps all knobs in one place.
"""
import os

class GraphConfig:
    def __init__(self, settings):
        self.RRD_DIR     = settings.get("rrd_directory", "data")
        self.GRAPH_DIR   = settings.get("graph_output_directory", "html/graphs")
        self.TRACE_DIR   = settings.get("traceroute_directory", "traceroute")
        self.MAX_HOPS    = settings.get("max_hops", 30)
        self.WIDTH       = settings.get("graph_width", 800)
        self.HEIGHT      = settings.get("graph_height", 200)
        self.TIME_RANGES = settings.get("graph_time_ranges", [])
        self.DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]

        g = settings.get("graph_generation", {})
        self.EXECUTOR_KIND = str(g.get("executor", "process")).lower()        # process|thread
        self.SKIP_UNCHANGED = bool(g.get("skip_unchanged", True))
        self.RECENT_SAFETY_SECONDS = int(g.get("recent_safety_seconds", 120))
        self.NICENESS = int(g.get("niceness", 5))
        self.CPU_AFFINITY = str(g.get("cpu_affinity", "none")).lower()        # none|spread
        self.USE_RRD_LOCK = bool(g.get("use_rrd_lock", True))

        # cadence
        self.SUMMARY_EVERY = int(g.get("summary_interval_runs", 1))
        self.HOPS_EVERY    = int(g.get("hop_interval_runs", 5))

        # parallelism: supports "auto"
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

        # state file for cadence counter
        self.STATE_PATH = os.path.join(self.GRAPH_DIR, ".graph_state.json")

def load_graph_config(settings) -> GraphConfig:
    cfg = GraphConfig(settings)
    # niceness (best-effort)
    try:
        if cfg.NICENESS:
            os.nice(cfg.NICENESS)
    except Exception:
        pass
    return cfg
