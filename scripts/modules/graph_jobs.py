#!/usr/bin/env python3
"""
graph_jobs.py

Plan the list of graph "jobs" for the executor to run, and perform
lightweight cleanup of stale PNGs.

This version:
  - Plans ONLY "summary" jobs (no per-hop jobs).
  - Cleans inside the per-IP folder: <GRAPH_DIR>/<ip>/...
"""

import os
from modules.graph_utils import get_labels

def _clean_old_graphs_ip_dir(graph_root: str, ip: str, expected_pngs: set):
    """
    Delete old PNGs for a given IP that are not in 'expected_pngs'.
    Operates inside <graph_root>/<ip>/.
    """
    ip_dir = os.path.join(graph_root, ip)
    try:
        for fname in os.listdir(ip_dir):
            if not (fname.startswith(f"{ip}_") and fname.endswith(".png")):
                continue
            if fname not in expected_pngs:
                try:
                    os.remove(os.path.join(ip_dir, fname))
                except Exception:
                    # Non-fatal; skip on error to avoid breaking planning.
                    pass
    except FileNotFoundError:
        # Nothing to clean if dir doesn't exist yet.
        pass

def _list_rrd_ds(rrdtool, rrd_path: str):
    """
    Return a set of DS names present in the RRD. We keep it for future checks
    (e.g., if you later need to probe whether a DS exists before planning).
    """
    try:
        info = rrdtool.info(rrd_path)
        ds = set()
        for k in info.keys():
            if k.startswith("ds[") and k.endswith("].type"):
                ds.add(k[3:-6])  # 'ds[hop0_avg].type' → 'hop0_avg'
        return ds
    except Exception:
        return set()

def plan_jobs_for_targets(settings, cfg, do_summary: bool, do_hops: bool):
    """
    Build ("summary", args) jobs for each target IP, metric, and time range.

    Parameters:
      settings: project settings dict (already loaded)
      cfg:      GraphConfig (parsed view of relevant settings)
      do_summary: bool  (cadence control) — if False, plan no summary jobs
      do_hops:   bool  (ignored in this build; kept for signature stability)

    Returns:
      list[tuple[str, tuple]]: [("summary", args), ...]
    """
    import yaml, rrdtool  # local import keeps module import time low

    # Load targets from YAML
    try:
        with open("mtr_targets.yaml") as f:
            targets = yaml.safe_load(f).get("targets", [])
    except Exception:
        targets = []

    jobs = []
    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue

        rrd_path = os.path.join(cfg.RRD_DIR, f"{ip}.rrd")
        if not os.path.exists(rrd_path):
            # Skip targets that haven't produced an RRD yet
            continue

        # Legend labels (stabilized from traceroute stats JSON if available)
        hops = get_labels(ip, traceroute_dir=cfg.TRACE_DIR)
        if not hops:
            # If we have no legend labels (no traceroute yet), you can either skip or
            # fall back to numeric labels; we choose to skip to avoid blank legends.
            continue

        # Expected filenames for cleanup (now inside per-IP folder)
        expected = set()
        for metric in cfg.DATA_SOURCES:
            for rng in cfg.TIME_RANGES:
                label = rng.get("label")
                if not label:
                    continue
                expected.add(f"{ip}_{metric}_{label}.png")

        # Clean old/renamed PNGs inside <GRAPH_DIR>/<ip>/
        _clean_old_graphs_ip_dir(cfg.GRAPH_DIR, ip, expected)

        # If summary graphs are gated by cadence, honor it here
        if not do_summary:
            continue

        # Optionally inspect DS present in RRD (kept for future logic)
        _ = _list_rrd_ds(rrdtool, rrd_path)

        # Plan one summary job per (metric, time-range)
        for metric in cfg.DATA_SOURCES:
            for rng in cfg.TIME_RANGES:
                label = rng.get("label")
                seconds = rng.get("seconds")
                if not label or not seconds:
                    continue

                jobs.append(("summary", (
                    ip, rrd_path, metric, label, seconds, hops,
                    cfg.WIDTH, cfg.HEIGHT, cfg.SKIP_UNCHANGED, cfg.RECENT_SAFETY_SECONDS,
                    cfg.TRACE_DIR, cfg.USE_RRD_LOCK, cfg.EXECUTOR_KIND, cfg.GRAPH_DIR,
                    cfg.CPU_AFFINITY
                )))

    return jobs
