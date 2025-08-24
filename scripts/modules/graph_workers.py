#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/graph_workers.py
Robust summary graph worker that only graphs DS actually present in the RRD.
"""

import os, re, time, math, threading
import rrdtool

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
    """Collect DS names like 'hop3_avg' from rrdtool.info(rrd)."""
    try:
        info = rrdtool.info(rrd_path)
    except rrdtool.OperationalError:
        return set()
    ds = set()
    for k in info.keys():
        m = re.match(r"^ds\[(.+?)\]\.", str(k))
        if m:
            ds.add(m.group(1))
    return ds

def graph_summary_work(args):
    """
    Args tuple:
      (ip, rrd_path, metric, label, seconds, hops, width, height,
       skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind,
       graph_dir, cpu_affinity)
    """
    (ip, rrd_path, metric, label, seconds, hops, width, height,
     skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind,
     graph_dir, cpu_affinity) = args

    _maybe_spread_affinity(cpu_affinity)

    ip_dir = os.path.join(graph_dir, ip)
    os.makedirs(ip_dir, exist_ok=True)
    png = os.path.join(ip_dir, f"{ip}_{metric}_{label}.png")

    t0 = time.time()
    if _should_skip(png, rrd_path, ip, trace_dir, skip_unchanged, recent_safety_seconds):
        return ("skipped", png, time.time() - t0)

    ds_present = _existing_ds_set(rrd_path)

    defs, lines = [], []
    for hop_index, hop_label in (hops or []):
        ds_name = f"hop{hop_index}_{metric}"
        if ds_name not in ds_present:
            continue  # ignore missing DS for this hop/metric
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{_color(hop_index)}:{_sanitize(hop_label)}")

    if not defs:
        # Nothing to draw for this metric in this RRD
        return ("skipped", png, time.time() - t0)

    # Put graph options FIRST (rrdtool is picky), then DEF/LINEs.
    cmd = [
        f"--title={ip} - {metric.upper()} ({label})",
        f"--width={int(width)}",
        f"--height={int(height)}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{int(seconds)}",
    ] + defs + lines

    try:
        if exec_kind == "thread" and use_lock:
            with RRD_LOCK:
                rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError:
        return ("error", png, time.time() - t0)
