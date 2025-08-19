#!/usr/bin/env python3
"""
Worker functions executed in pool workers (processes by default).
Kept top-level and picklable.
"""
import os, time, re, math, threading
import rrdtool

RRD_LOCK = threading.Lock()  # only used if executor=thread & use_rrd_lock=True

def _sanitize(label: str) -> str:
    return re.sub(r'[:\\\'"]', '-', label)

def _color(hop_index: int) -> str:
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"{r:02x}{g:02x}{b:02x}"

def _traceroute_latest_mtime(trace_dir: str, ip: str) -> float:
    latest = 0.0
    try:
        if not os.path.isdir(trace_dir): return 0.0
        for fn in os.listdir(trace_dir):
            if not fn.startswith(ip): continue
            p = os.path.join(trace_dir, fn)
            try: latest = max(latest, os.path.getmtime(p))
            except Exception: pass
    except Exception:
        pass
    return latest

def _should_skip(png: str, rrd: str, ip: str, trace_dir: str, skip_unchanged: bool, recent_safety: int) -> bool:
    if not skip_unchanged or not os.path.exists(png): return False
    try:
        png_mtime = os.path.getmtime(png)
        rrd_mtime = os.path.getmtime(rrd) if os.path.exists(rrd) else 0
        tr_mtime  = _traceroute_latest_mtime(trace_dir, ip)
        newest    = max(rrd_mtime, tr_mtime)
        return png_mtime >= newest and (time.time() - newest) > recent_safety
    except Exception:
        return False

def _maybe_spread_affinity(mode: str):
    if mode != "spread": return
    try:
        if hasattr(os, "sched_setaffinity") and hasattr(os, "sched_getaffinity"):
            cpus = sorted(list(os.sched_getaffinity(0)))
            if not cpus: return
            core = cpus[os.getpid() % len(cpus)]
            os.sched_setaffinity(0, {core})
    except Exception:
        pass

def graph_summary_work(args):
    (ip, rrd_path, metric, label, seconds, hops, width, height,
     skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind, graph_dir, cpu_affinity) = args

    _maybe_spread_affinity(cpu_affinity)

    png = os.path.join(graph_dir, f"{ip}_{metric}_{label}.png")
    t0 = time.time()
    if _should_skip(png, rrd_path, ip, trace_dir, skip_unchanged, recent_safety_seconds):
        return ("skipped", png, time.time() - t0)

    defs, lines = [], []
    for hop_index, raw_label in hops:
        ds_name = f"hop{hop_index}_{metric}"
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{_color(hop_index)}:{_sanitize(raw_label)}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({label})",
        f"--width={width}",
        f"--height={height}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{seconds}",
    ]

    try:
        if exec_kind == "thread" and use_lock:
            with RRD_LOCK: rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError:
        return ("error", png, time.time() - t0)

def graph_hop_work(args):
    (ip, rrd_path, hop_index, metric, label, seconds, ds_present, hop_label, width, height,
     skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind, graph_dir, cpu_affinity) = args

    _maybe_spread_affinity(cpu_affinity)

    ds_name = f"hop{hop_index}_{metric}"
    png = os.path.join(graph_dir, f"{ip}_hop{hop_index}_{metric}_{label}.png")
    t0 = time.time()

    if ds_name not in ds_present:
        return ("skipped", png, 0.0)
    if _should_skip(png, rrd_path, ip, trace_dir, skip_unchanged, recent_safety_seconds):
        return ("skipped", png, time.time() - t0)

    safe = _sanitize(hop_label)
    color = _color(hop_index)
    cmd = [
        f"DEF:v={rrd_path}:{ds_name}:AVERAGE",
        f"LINE1:v#{color}:{safe}",
        f"GPRINT:v:LAST:Last\\: %.1lf",
        f"GPRINT:v:MAX:Max\\: %.1lf",
        f"GPRINT:v:AVERAGE:Avg\\: %.1lf",
        "--units-exponent", "0",
        "--vertical-label", "Latency (ms)" if metric != "loss" else "Loss (%)",
        f"--title={ip} - Hop {hop_index} {metric.upper()} ({label})",
        f"--width={width}",
        f"--height={height}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{seconds}",
    ]

    try:
        if exec_kind == "thread" and use_lock:
            with RRD_LOCK: rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError:
        return ("error", png, time.time() - t0)
