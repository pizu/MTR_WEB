#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/graph_jobs.py

Purpose
-------
Plan the list of graph "jobs" for the executor to run, and perform lightweight
cleanup of stale PNGs inside per-IP subfolders.

Key changes (clean design)
-------------------------
- This module **does not** import label helpers from graph_utils.
- It **only reads** the already-written <traceroute>/<ip>_hops.json labels file.
- Single-writer principle: graph_utils remains the only writer of trace artifacts.

Expected cfg fields (GraphConfig)
---------------------------------
RRD_DIR:                directory containing <ip>.rrd files
TRACE_DIR:              directory containing <ip>_hops.json & *_hops_stats.json
GRAPH_DIR:              base directory where per-IP graph PNGs live (<GRAPH_DIR>/<ip>/...)
DATA_SOURCES:           iterable of RRD DS names to plot (e.g., {"hop0_avg", "hop0_best", ...})
TIME_RANGES:            iterable of {"label": str, "seconds": int}
WIDTH, HEIGHT:          PNG dimensions
SKIP_UNCHANGED:         bool (executor optimization)
RECENT_SAFETY_SECONDS:  int (avoid races with very recent data)
USE_RRD_LOCK:           bool
EXECUTOR_KIND:          str (executor hint)
CPU_AFFINITY:           optional (int or None)

Returns
-------
list[tuple[str, tuple]]: [("summary", args), ...] where args are consumed by graph workers.
"""

from __future__ import annotations

import os
import json
from typing import Dict, List, Tuple, Set


# -----------------------------------------------------------------------------
# Helpers: read labels from <TRACE_DIR>/<ip>_hops.json
# -----------------------------------------------------------------------------
def _read_labels_from_file(ip: str, traceroute_dir: str) -> Dict[int, str]:
    """
    Read labels for one IP from <traceroute>/<ip>_hops.json and return {hop_index: "N: label"}.
    Returns {} if file is missing or invalid. This module never writes labels.
    """
    path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    labels: Dict[int, str] = {}
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f) or []
            for rec in arr:
                n = int(rec.get("count", 0))
                if n >= 1:
                    labels[n] = f"{n}: {rec.get('host')}"
    except Exception:
        # Non-fatal: if labels are missing or malformed, we return {} and the caller can skip.
        return {}
    return labels


# -----------------------------------------------------------------------------
# Helpers: cleanup old PNGs inside <GRAPH_DIR>/<ip>/
# -----------------------------------------------------------------------------
def _clean_old_graphs_ip_dir(graph_root: str, ip: str, expected_pngs: Set[str]) -> None:
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


# -----------------------------------------------------------------------------
# Optional: inspect DS present in RRD (kept for future logic)
# -----------------------------------------------------------------------------
def _list_rrd_ds(rrdtool, rrd_path: str) -> Set[str]:
    """
    Return a set of DS names present in the RRD. Currently unused for planning,
    but kept for future checks (e.g., conditionally plan per-DS graphs).
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


# -----------------------------------------------------------------------------
# Main: plan summary jobs
# -----------------------------------------------------------------------------
def plan_jobs_for_targets(settings, cfg, do_summary: bool, do_hops: bool) -> List[Tuple[str, tuple]]:
    """
    Build ("summary", args) jobs for each target IP, metric, and time range.

    Parameters
    ----------
    settings : dict
        Project settings (already loaded). Not used here except for parity of signature.
    cfg : GraphConfig-like
        Object/namespace with attributes described in the module docstring.
    do_summary : bool
        If False, plan no summary jobs.
    do_hops : bool
        Ignored in this build; kept only for signature stability.

    Returns
    -------
    list[tuple[str, tuple]]
        A list of ("summary", args) job tuples for the executor.
    """
    import yaml, rrdtool  # local import keeps module import time low

    # Load targets from YAML
    try:
        with open("mtr_targets.yaml") as f:
            targets = yaml.safe_load(f).get("targets", [])
    except Exception:
        targets = []

    jobs: List[Tuple[str, tuple]] = []

    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue

        rrd_path = os.path.join(cfg.RRD_DIR, f"{ip}.rrd")
        if not os.path.exists(rrd_path):
            # Skip targets that haven't produced an RRD yet
            continue

        # Legend labels (stabilized from traceroute stats JSON if available)
        hops = _read_labels_from_file(ip, traceroute_dir=cfg.TRACE_DIR)
        if not hops:
            # If we have no legend labels (no traceroute yet), skip to avoid blank legends.
            continue

        # Expected filenames for cleanup (now inside per-IP folder)
        expected: Set[str] = set()
        for metric in cfg.DATA_SOURCES:
            for rng in cfg.TIME_RANGES:
                label = rng.get("label")
                if not label:
                    continue
                expected.add(f"{ip}_{metric}_{label}.png")

        # Clean old/renamed PNGs inside <GRAPH_DIR>/<ip>/
        _clean_old_graphs_ip_dir(cfg.GRAPH_DIR, ip, expected)

        # Cadence gate: bail early if summary is disabled
        if not do_summary:
            continue

        # Optional: inspect DS present in RRD (reserved for future logic)
        _ = _list_rrd_ds(rrdtool, rrd_path)

        # Plan one summary job per (metric, time-range)
        for metric in cfg.DATA_SOURCES:
            for rng in cfg.TIME_RANGES:
                label = rng.get("label")
                seconds = rng.get("seconds")
                if not label or not seconds:
                    continue

                jobs.append((
                    "summary",
                    (
                        ip, rrd_path, metric, label, seconds, hops,
                        cfg.WIDTH, cfg.HEIGHT, cfg.SKIP_UNCHANGED, cfg.RECENT_SAFETY_SECONDS,
                        cfg.TRACE_DIR, cfg.USE_RRD_LOCK, cfg.EXECUTOR_KIND, cfg.GRAPH_DIR,
                        cfg.CPU_AFFINITY
                    )
                ))

    return jobs
