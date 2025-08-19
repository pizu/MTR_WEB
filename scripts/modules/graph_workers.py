#!/usr/bin/env python3
"""
graph_workers.py

Worker functions executed in the pool (processes by default).
This trimmed version:
  - Generates ONLY "summary" graphs (multi-hop overlays).
  - Writes PNGs into per-IP subfolders: <GRAPH_DIR>/<ip>/<ip>_<metric>_<label>.png
  - Removes the legacy per-hop worker and associated code.

Why per-IP subfolders?
  - Keeps html/graphs tidy
  - Avoids name collisions across targets
  - Makes it trivial for the HTML builder to glob all graphs for a given IP

Notes:
  - We keep an optional RRD lock only if you run with threads AND use_rrd_lock==True.
  - We implement a conservative skip-unchanged that considers both RRD and traceroute files.
"""

import os
import time
import re
import math
import threading
import rrdtool

# ---------------------------------------------------------------------------
# Optional lock for thread executor (no effect with process executor)
# ---------------------------------------------------------------------------
RRD_LOCK = threading.Lock()

def _sanitize(label: str) -> str:
    """
    Replace characters that rrdtool treats specially in legend/text.
    """
    return re.sub(r'[:\\\'"]', '-', label)

def _color(hop_index: int) -> str:
    """
    Deterministic pseudo-distinct color for each hop line.
    """
    # Smooth sin palette; good enough for multi-line overlays
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"{r:02x}{g:02x}{b:02x}"

def _traceroute_latest_mtime(trace_dir: str, ip: str) -> float:
    """
    Return the most recent modification time among traceroute artifacts for this IP.
    If none exist, returns 0.0.
    """
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
    """
    Decide whether to skip rendering:
      - If skip_unchanged is False: never skip.
      - If output PNG is missing: never skip.
      - Otherwise compare PNG mtime with the newest of (RRD mtime, traceroute files mtime).
        Only skip when PNG is newer/equal AND the newest input is older than 'recent_safety' seconds,
        to avoid racing a just-updated RRD/traceroute.
    """
    if not skip_unchanged or not os.path.exists(png):
        return False
    try:
        png_mtime = os.path.getmtime(png)
        rrd_mtime = os.path.getmtime(rrd) if os.path.exists(rrd) else 0
        tr_mtime  = _traceroute_latest_mtime(trace_dir, ip)
        newest    = max(rrd_mtime, tr_mtime)
        # Skip only if PNG is at least as new AND inputs are not "fresh"
        return png_mtime >= newest and (time.time() - newest) > recent_safety
    except Exception:
        # On any error, be safe and redraw.
        return False

def _maybe_spread_affinity(mode: str):
    """
    Best-effort CPU pinning across workers when mode == 'spread'.
    No-ops on platforms without sched_setaffinity.
    """
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
        # Non-fatal if it fails.
        pass

# -----------------------------------------------------------------------------
# PUBLIC WORKER: SUMMARY GRAPH
# -----------------------------------------------------------------------------
def graph_summary_work(args):
    """
    Render a multi-hop overlay for a single metric & time-range for one IP.

    Args tuple (positional to keep picklable/fast):
      ip: str
      rrd_path: str
      metric: str                 # e.g., 'avg', 'last', 'best', 'loss'
      label: str                  # human tag for time range (e.g., '15m', '1h')
      seconds: int                # range window in seconds
      hops: list[(int, str)]      # list of (hop_index, label) pairs for legend
      width: int
      height: int
      skip_unchanged: bool
      recent_safety_seconds: int
      trace_dir: str
      use_lock: bool              # only used when exec_kind == 'thread'
      exec_kind: str              # 'process' or 'thread'
      graph_dir: str              # base dir for HTML graphs (e.g., 'html/graphs')
      cpu_affinity: str           # 'none' or 'spread'

    Returns: ("ok"|"skipped"|"error", png_path: str, elapsed_seconds: float)
    """
    (ip, rrd_path, metric, label, seconds, hops, width, height,
     skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind,
     graph_dir, cpu_affinity) = args

    _maybe_spread_affinity(cpu_affinity)

    # --- Per-IP subfolder: <graph_dir>/<ip> ---
    ip_dir = os.path.join(graph_dir, ip)
    # Ensure it exists before we attempt to write PNGs
    os.makedirs(ip_dir, exist_ok=True)

    # Output filename pattern remains the same, but now lives inside ip_dir
    png = os.path.join(ip_dir, f"{ip}_{metric}_{label}.png")

    # Skip check (considers RRD + traceroute freshness)
    t0 = time.time()
    if _should_skip(png, rrd_path, ip, trace_dir, skip_unchanged, recent_safety_seconds):
        return ("skipped", png, time.time() - t0)

    # Build rrdtool command: one DEF + LINE per hop
    defs, lines = [], []
    for hop_index, raw_label in hops:
        ds_name = f"hop{hop_index}_{metric}"        # must match how RRD DS are named
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        # Keep labels safe for rrdtool legend (avoid colons/quotes)
        lines.append(f"LINE1:{ds_name}#{_color(hop_index)}:{_sanitize(raw_label)}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({label})",
        f"--width={width}",
        f"--height={height}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{seconds}",
    ]

    # Run graph generation with optional intra-process lock for threaded mode
    try:
        if exec_kind == "thread" and use_lock:
            with RRD_LOCK:
                rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError:
        # Keep it tight: caller tallies errors; logs are handled in the parent.
        return ("error", png, time.time() - t0)
