#!/usr/bin/env python3
"""
graph_generator.py — safer & lighter

- Default: ProcessPoolExecutor (per-task process isolation → avoids GLib/pango thread bugs)
- Optional: ThreadPoolExecutor (if selected) + global lock around rrdtool.graph()
- Skip-unchanged PNGs based on RRD/traceroute mtimes (with safety buffer)
- Bounded parallelism + per-run timing logs
- Lower niceness so we don't starve the host
"""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # import modules/*

import yaml
import rrdtool
import re
import math
import time
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from modules.utils import load_settings, setup_logger
from modules.graph_utils import get_labels

# -------------------------
# Settings / logger
# -------------------------
settings = load_settings("mtr_script_settings.yaml")
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("graph_generator", log_directory, "graph_generator.log", settings=settings)

RRD_DIR = settings.get("rrd_directory", "data")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
TRACEROUTE_DIR = settings.get("traceroute_directory", "traceroute")
MAX_HOPS = settings.get("max_hops", 30)
GRAPH_WIDTH = settings.get("graph_width", 800)
GRAPH_HEIGHT = settings.get("graph_height", 200)
TIME_RANGES = settings.get("graph_time_ranges", [])
DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]

CPU_COUNT = os.cpu_count() or 2
GRAPH_CFG = settings.get("graph_generation", {})

# parse parallelism safely
raw_parallelism = GRAPH_CFG.get("parallelism", 2)
if isinstance(raw_parallelism, str):
    if raw_parallelism.strip().lower() == "auto":
        PARALLELISM = os.cpu_count() or 2
    else:
        try:
            PARALLELISM = int(raw_parallelism)
        except ValueError:
            PARALLELISM = 2
else:
    try:
        PARALLELISM = int(raw_parallelism)
    except Exception:
        PARALLELISM = 2

EXECUTOR_KIND = str(GRAPH_CFG.get("executor", "process")).lower()
USE_RRD_LOCK = bool(GRAPH_CFG.get("use_rrd_lock", True))
SKIP_UNCHANGED = bool(GRAPH_CFG.get("skip_unchanged", True))
RECENT_SAFETY_SECONDS = int(GRAPH_CFG.get("recent_safety_seconds", 120))
NICENESS = int(GRAPH_CFG.get("niceness", 5))



os.makedirs(GRAPH_DIR, exist_ok=True)

# Lower priority (nice +N) so we are kinder to the CPU
try:
    if NICENESS:
        os.nice(NICENESS)
        logger.info(f"Set niceness to +{NICENESS}")
except Exception as e:
    logger.warning(f"Could not set niceness: {e}")

# Global lock (effective only with threads; harmless with processes)
RRD_LOCK = threading.Lock()

# -------------------------
# Helpers
# -------------------------
def sanitize_label(label: str) -> str:
    return re.sub(r'[:\\\'"]', '-', label)

def color_for_hop(hop_index: int) -> str:
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"{r:02x}{g:02x}{b:02x}"

def traceroute_latest_mtime(ip: str) -> float:
    latest = 0.0
    try:
        if not os.path.isdir(TRACEROUTE_DIR):
            return 0.0
        for fn in os.listdir(TRACEROUTE_DIR):
            if fn.startswith(ip):
                p = os.path.join(TRACEROUTE_DIR, fn)
                try:
                    latest = max(latest, os.path.getmtime(p))
                except Exception:
                    pass
    except Exception:
        pass
    return latest

def png_path_summary(ip: str, metric: str, label: str) -> str:
    return os.path.join(GRAPH_DIR, f"{ip}_{metric}_{label}.png")

def png_path_hop(ip: str, hop_index: int, metric: str, label: str) -> str:
    return os.path.join(GRAPH_DIR, f"{ip}_hop{hop_index}_{metric}_{label}.png")

def should_skip_png(png: str, rrd: str, ip: str) -> bool:
    if not SKIP_UNCHANGED or not os.path.exists(png):
        return False
    try:
        png_mtime = os.path.getmtime(png)
        rrd_mtime = os.path.getmtime(rrd) if os.path.exists(rrd) else 0
        tr_mtime  = traceroute_latest_mtime(ip)
        newest_source = max(rrd_mtime, tr_mtime)
        if png_mtime >= newest_source and (time.time() - newest_source) > RECENT_SAFETY_SECONDS:
            return True
    except Exception:
        return False
    return False

def list_ds(rrd_path: str):
    try:
        info = rrdtool.info(rrd_path)
        ds = set()
        for k in info.keys():
            if k.startswith("ds[") and k.endswith("].type"):
                ds.add(k[3:-6])  # extract name inside brackets
        return ds
    except Exception as e:
        logger.warning(f"RRD info failed for {rrd_path}: {e}")
        return set()

def clean_old_graphs(ip: str, expected_pngs: set):
    try:
        for fname in os.listdir(GRAPH_DIR):
            if fname.startswith(f"{ip}_") and fname.endswith(".png"):
                if fname not in expected_pngs:
                    try:
                        os.remove(os.path.join(GRAPH_DIR, fname))
                        logger.info(f"[CLEANED] {fname}")
                    except Exception as e:
                        logger.warning(f"[SKIP CLEANUP] {fname}: {e}")
    except FileNotFoundError:
        pass

def try_set_affinity_spread():
    """
    On Linux, pin the current process to one CPU in a round-robin pattern.
    Not required (the OS already balances), but can help avoid cache ping-pong.
    """
    try:
        # Prefer Python's built-in affinity if available
        if hasattr(os, "sched_setaffinity") and hasattr(os, "sched_getaffinity"):
            cpus = sorted(list(os.sched_getaffinity(0)))
            if not cpus:
                return
            core = cpus[os.getpid() % len(cpus)]
            os.sched_setaffinity(0, {core})
        else:
            # Optional: psutil fallback if you have it installed
            try:
                import psutil
                p = psutil.Process()
                cpus = list(range(psutil.cpu_count(logical=True) or 1))
                core = cpus[os.getpid() % len(cpus)]
                p.cpu_affinity([core])
            except Exception:
                pass
    except Exception:
        pass
# -------------------------
# Worker functions (must be top-level to be picklable for ProcessPool)
# -------------------------
def _graph_summary_work(args):
    """
    Run inside a worker (thread or process). Returns (status, png_path, seconds_taken).
    """
    (ip, rrd_path, metric, label, seconds, hops, width, height,
     skip_unchanged, recent_safety_seconds, tracer_dir, use_lock, exec_kind) = args

    # Honor CPU affinity setting
    if str(settings.get("graph_generation", {}).get("cpu_affinity", "none")).lower() == "spread":
        try_set_affinity_spread()

    # Reconstruct helpers needed in child process
    def _tr_mtime(ip_):
        # Honor CPU affinity setting
        if str(settings.get("graph_generation", {}).get("cpu_affinity", "none")).lower() == "spread":
            try_set_affinity_spread()

        latest = 0.0
        try:
            if not os.path.isdir(tracer_dir):
                return 0.0
            for fn in os.listdir(tracer_dir):
                if fn.startswith(ip_):
                    p = os.path.join(tracer_dir, fn)
                    try:
                        latest = max(latest, os.path.getmtime(p))
                    except Exception:
                        pass
        except Exception:
            pass
        return latest

    def _should_skip(png_, rrd_, ip_):
        if not skip_unchanged or not os.path.exists(png_):
            return False
        try:
            png_mtime = os.path.getmtime(png_)
            rrd_mtime = os.path.getmtime(rrd_) if os.path.exists(rrd_) else 0
            tr_mtime  = _tr_mtime(ip_)
            newest_source = max(rrd_mtime, tr_mtime)
            return png_mtime >= newest_source and (time.time() - newest_source) > recent_safety_seconds
        except Exception:
            return False

    png = os.path.join(GRAPH_DIR, f"{ip}_{metric}_{label}.png")
    t0 = time.time()
    if _should_skip(png, rrd_path, ip):
        return ("skipped", png, time.time() - t0)

    defs, lines = [], []
    for hop_index, raw_label in hops:
        ds_name = f"hop{hop_index}_{metric}"
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{color_for_hop(hop_index)}:{sanitize_label(raw_label)}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({label})",
        f"--width={width}",
        f"--height={height}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{seconds}",
    ]

    try:
        # If running threads and lock requested, serialize rrdtool.graph within the process
        if exec_kind == "thread" and use_lock:
            # Note: in ProcessPool this lock is process-local (harmless).
            with RRD_LOCK:
                rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError as e:
        return ("error", png, time.time() - t0)

def _graph_hop_work(args):
    """
    Run inside a worker (thread or process). Returns (status, png_path, seconds_taken).
    """
    (ip, rrd_path, hop_index, metric, label, seconds, ds_present, hop_label, width, height,
     skip_unchanged, recent_safety_seconds, tracer_dir, use_lock, exec_kind) = args

    # Honor CPU affinity setting
    if str(settings.get("graph_generation", {}).get("cpu_affinity", "none")).lower() == "spread":
        try_set_affinity_spread()

    def _tr_mtime(ip_):
        latest = 0.0
        try:
            if not os.path.isdir(tracer_dir):
                return 0.0
            for fn in os.listdir(tracer_dir):
                if fn.startswith(ip_):
                    p = os.path.join(tracer_dir, fn)
                    try:
                        latest = max(latest, os.path.getmtime(p))
                    except Exception:
                        pass
        except Exception:
            pass
        return latest

    def _should_skip(png_, rrd_, ip_):
        if not skip_unchanged or not os.path.exists(png_):
            return False
        try:
            png_mtime = os.path.getmtime(png_)
            rrd_mtime = os.path.getmtime(rrd_) if os.path.exists(rrd_) else 0
            tr_mtime  = _tr_mtime(ip_)
            newest_source = max(rrd_mtime, tr_mtime)
            return png_mtime >= newest_source and (time.time() - newest_source) > recent_safety_seconds
        except Exception:
            return False

    ds_name = f"hop{hop_index}_{metric}"
    png = os.path.join(GRAPH_DIR, f"{ip}_hop{hop_index}_{metric}_{label}.png")
    t0 = time.time()

    if ds_name not in ds_present:
        return ("skipped", png, 0.0)
    if _should_skip(png, rrd_path, ip):
        return ("skipped", png, time.time() - t0)

    safe_label = sanitize_label(hop_label)
    color = color_for_hop(hop_index)
    cmd = [
        f"DEF:v={rrd_path}:{ds_name}:AVERAGE",
        f"LINE1:v#{color}:{safe_label}",
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
            with RRD_LOCK:
                rrdtool.graph(png, *cmd)
        else:
            rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError as e:
        return ("error", png, time.time() - t0)

# -------------------------
# Main
# -------------------------
def main():
    # Load targets
    try:
        with open("mtr_targets.yaml") as f:
            targets = yaml.safe_load(f).get("targets", [])
    except Exception as e:
        logger.error(f"[ERROR] Failed to load mtr_targets.yaml: {e}")
        targets = []

    t_start = time.time()
    total = skipped = errors = 0
    jobs = []

    # Plan jobs
    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue

        rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
        if not os.path.exists(rrd_path):
            logger.warning(f"[SKIP] No RRD for {ip}")
            continue

        hops = get_labels(ip, traceroute_dir=TRACEROUTE_DIR)
        if not hops:
            logger.warning(f"[SKIP] No valid traceroute data for {ip}")
            continue

        # Cleanup planning
        expected = set()
        for metric in DATA_SOURCES:
            for rng in TIME_RANGES:
                label = rng.get("label")
                if not label:
                    continue
                expected.add(f"{ip}_{metric}_{label}.png")
                for hop_index, _ in hops:
                    expected.add(f"{ip}_hop{hop_index}_{metric}_{label}.png")
        clean_old_graphs(ip, expected)

        ds_present = list_ds(rrd_path)

        # Add jobs
        for metric in DATA_SOURCES:
            for rng in TIME_RANGES:
                label = rng.get("label")
                seconds = rng.get("seconds")
                if not label or not seconds:
                    continue

                jobs.append((
                    "summary",
                    (ip, rrd_path, metric, label, seconds, hops,
                     GRAPH_WIDTH, GRAPH_HEIGHT, SKIP_UNCHANGED, RECENT_SAFETY_SECONDS,
                     TRACEROUTE_DIR, USE_RRD_LOCK, EXECUTOR_KIND)
                ))

                for hop_index, hop_label in hops:
                    jobs.append((
                        "hop",
                        (ip, rrd_path, hop_index, metric, label, seconds, ds_present, hop_label,
                         GRAPH_WIDTH, GRAPH_HEIGHT, SKIP_UNCHANGED, RECENT_SAFETY_SECONDS,
                         TRACEROUTE_DIR, USE_RRD_LOCK, EXECUTOR_KIND)
                    ))

    logger.info(f"Planned {len(jobs)} graph jobs (executor={EXECUTOR_KIND}, parallelism={PARALLELISM}, skip_unchanged={SKIP_UNCHANGED})")

    # Choose executor
    Executor = ProcessPoolExecutor if EXECUTOR_KIND == "process" else ThreadPoolExecutor

    # Run jobs
    with Executor(max_workers=PARALLELISM) as pool:
        futures = []
        for kind, args in jobs:
            if kind == "summary":
                futures.append(pool.submit(_graph_summary_work, args))
            else:
                futures.append(pool.submit(_graph_hop_work, args))

        for fut in as_completed(futures):
            status, _, dt = fut.result()
            total += 1
            if status == "skipped":
                skipped += 1
            elif status == "error":
                errors += 1

    wall = time.time() - t_start
    logger.info(f"Graph gen finished: jobs={total}, skipped={skipped}, errors={errors}, wall={wall:.2f}s")

if __name__ == "__main__":
    main()
