#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/graph_workers.py
========================

Summary
-------
Worker that renders multi-hop overlay graphs directly from the per-target RRD.
This version is robust against schema mismatches:
- It discovers DS names in the RRD via `rrdtool.info()`.
- It only graphs hop/metric pairs that actually exist in the RRD.
- If a requested metric doesn't exist at all, it returns ("skipped", ...) instead
  of failing the job, so your run summary shows SKIPPED (not ERROR).

Output layout
-------------
PNG files are written under per-IP subfolders:

    <graphs>/<ip>/<ip>_<metric>_<label>.png

Inputs (args tuple)
-------------------
ip: str
rrd_path: str
metric: str                 # e.g., "avg", "loss", "best", "worst"
label: str                  # time window tag, e.g., "1h"
seconds: int
hops: list[(int, str)]      # (hop_index, legend_label)
width: int
height: int
skip_unchanged: bool
recent_safety_seconds: int
trace_dir: str
use_lock: bool              # only used for thread executor
exec_kind: str              # 'process' or 'thread'
graph_dir: str
cpu_affinity: str           # 'none' or 'spread'

Return
------
("ok"|"skipped"|"error", png_path: str, elapsed_seconds: float)
"""

import os
import re
import time
import math
import threading
import rrdtool

# ---------------------------------------------------------------------------
# Optional lock for thread executor (no effect with process executor)
# ---------------------------------------------------------------------------
RRD_LOCK = threading.Lock()


def _sanitize(label: str) -> str:
    return re.sub(r'[:\\\'"]', '-', label or '')


def _color(hop_index: int) -> str:
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"{r:02x}{g:02x}{b:02x}"


def _traceroute_latest_mtime(trace_dir: str, ip: str) -> float:
    latest = 0.0
    try:
        if not os.path.isdir(trace_dir):
            return 0.0
        for fn in os.listdir(trace_dir):
            if not fn.startswith(ip):
                continue
            p = os.path.join(trace_dir, fn)
            try:
                latest = max(latest, os.path.getmtime(p))
            except Exception:
                pass
    except Exception:
        pass
    return latest


def _should_skip(png: str, rrd: str, ip: str, trace_dir: str,
                 skip_unchanged: bool, recent_safety: int) -> bool:
    if not skip_unchanged or not os.path.exists(png):
        return False
    try:
        png_mtime = os.path.getmtime(png)
        rrd_mtime = os.path.getmtime(rrd) if os.path.exists(rrd) else 0
        tr_mtime  = _traceroute_latest_mtime(trace_dir, ip)
        newest    = max(rrd_mtime, tr_mtime)
        return png_mtime >= newest and (time.time() - newest) > recent_safety
    except Exception:
        return False


def _maybe_spread_affinity(mode: str):
    if mode != "spread":
        return
    try:
        if hasattr(os, "sched_setaffinity") and hasattr(os, "sched_getaffinity"):
            cpus = sorted(list(os.sched_getaffinity(0)))
            if not cpus:
                return
            core = cpus[os.getpid() % len(cpus)]
            os.sched_setaffinity(0, {core})
    except Exception:
        pass


def _existing_ds_set(rrd_path: str) -> set[str]:
    """
    Build the set of DS names present in the RRD, e.g. {'hop0_avg','hop0_loss',...}.
    We parse keys like 'ds[hop0_avg].type' from `rrdtool.info`.
    """
    try:
        info = rrdtool.info(rrd_path)
    except rrdtool.OperationalError:
        return set()
    ds = set()
    for k in info.keys():
        # rrdtool.info uses keys like: 'ds[<name>].type'
        m = re.match(r"^ds\[(.+?)\]\.", str(k))
        if m:
            ds.add(m.group(1))
    return ds


# -----------------------------------------------------------------------------
# PUBLIC WORKER: SUMMARY GRAPH
# -----------------------------------------------------------------------------
def graph_summary_work(args):
    (ip, rrd_path, metric, label, seconds, hops, width, height,
     skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind,
     graph_dir, cpu_affinity) = args

    _maybe_spread_affinity(cpu_affinity)

    # Per-IP subfolder
    ip_dir = os.path.join(graph_dir, ip)
    os.makedirs(ip_dir, exist_ok=True)
    png = os.path.join(ip_dir, f"{ip}_{metric}_{label}.png")

    # Skip check
    t0 = time.time()
    if _should_skip(png, rrd_path, ip, trace_dir, skip_unchanged, recent_safety_seconds):
        return ("skipped", png, time.time() - t0)

    # Discover actual DS names present in the RRD and only graph those
    ds_present = _existing_ds_set(rrd_path)

    defs, lines = [], []
    for hop_index, raw_label in (hops or []):
        ds_name = f"hop{hop_index}_{metric}"
        if ds_name not in ds_present:
            # silently ignore this hop for this metric
            continue
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{_color(hop_index)}:{_sanitize(raw_label)}")

    # If NOTHING to graph for this metric, mark as skipped (not error)
    if not defs:
        return ("skipped", png, time.time() - t0)

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({label})",
        f"--width={width}",
        f"--height={height}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{int(seconds)}",
    ]

    try:
        if exec_kind == "thread" and use_lock:
            with RRD_LOCK:
                rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError:
        return ("error", png, time.time() - t0)
