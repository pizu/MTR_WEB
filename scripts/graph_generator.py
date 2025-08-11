#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # So modules/* is importable

import yaml
import rrdtool
import re
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from modules.utils import load_settings, setup_logger
from modules.graph_utils import get_labels

# ---- Settings / logger ----
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

# New tuning block
GRAPH_CFG = settings.get("graph_generation", {})
PARALLELISM = int(GRAPH_CFG.get("parallelism", 2))             # render N graphs at once
SKIP_UNCHANGED = bool(GRAPH_CFG.get("skip_unchanged", True))   # skip if PNG newer than sources
RECENT_SAFETY_SECONDS = int(GRAPH_CFG.get("recent_safety_seconds", 120))
NICENESS = int(GRAPH_CFG.get("niceness", 5))

os.makedirs(GRAPH_DIR, exist_ok=True)

# Lower priority so it won't starve the box
try:
    if NICENESS:
        os.nice(NICENESS)
        logger.info(f"Set niceness to +{NICENESS}")
except Exception as e:
    logger.warning(f"Could not set niceness: {e}")

# ---- helpers ----
def sanitize_label(label: str) -> str:
    return re.sub(r'[:\\\'"]', '-', label)

def get_color_by_hop(hop_index: int) -> str:
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"{r:02x}{g:02x}{b:02x}"

def traceroute_latest_mtime(ip: str) -> float:
    """Return newest mtime for this IP's traceroute files (txt/json), or 0 if none."""
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
    # Keep current flat layout to avoid breaking HTML today
    return os.path.join(GRAPH_DIR, f"{ip}_{metric}_{label}.png")

def png_path_hop(ip: str, hop_index: int, metric: str, label: str) -> str:
    return os.path.join(GRAPH_DIR, f"{ip}_hop{hop_index}_{metric}_{label}.png")

def should_skip_png(png: str, rrd: str, ip: str) -> bool:
    if not SKIP_UNCHANGED:
        return False
    if not os.path.exists(png):
        return False
    try:
        png_mtime = os.path.getmtime(png)
        rrd_mtime = os.path.getmtime(rrd) if os.path.exists(rrd) else 0
        tr_mtime = traceroute_latest_mtime(ip)
        newest_source = max(rrd_mtime, tr_mtime)
        # skip only if PNG is newer *and* sources aren’t extremely fresh
        if png_mtime >= newest_source and (time.time() - newest_source) > RECENT_SAFETY_SECONDS:
            return True
    except Exception:
        return False
    return False

def list_ds(rrd_path: str):
    """Return a set of DS names in the RRD (fast-fail missing DS)."""
    try:
        info = rrdtool.info(rrd_path)
        # DS entries look like 'ds[hop0_avg].type'
        ds = set()
        for k in info.keys():
            if k.startswith("ds[") and k.endswith("].type"):
                name = k[3:-6]  # inside brackets
                ds.add(name)
        return ds
    except Exception as e:
        logger.warning(f"RRD info failed for {rrd_path}: {e}")
        return set()

def clean_old_graphs(ip: str, expected_pngs: set):
    """Only scan files that start with this IP to minimize I/O."""
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

# ---- graph workers ----
def job_summary(ip, rrd_path, metric, label, seconds, hops):
    t0 = time.time()
    png = png_path_summary(ip, metric, label)
    if should_skip_png(png, rrd_path, ip):
        return ("skipped", png, time.time() - t0)

    defs = []
    lines = []
    for hop_index, raw_label in hops:
        if hop_index > MAX_HOPS:
            continue
        ds_name = f"hop{hop_index}_{metric}"
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{get_color_by_hop(hop_index)}:{sanitize_label(raw_label)}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({label})",
        f"--width={GRAPH_WIDTH}",
        f"--height={GRAPH_HEIGHT}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{seconds}",
    ]

    try:
        rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError as e:
        logger.error(f"[ERROR] {ip} - {metric} ({label}): {e}")
        return ("error", png, time.time() - t0)

def job_hop(ip, rrd_path, hop_index, metric, label, seconds, ds_present: set, hop_label: str):
    t0 = time.time()
    ds_name = f"hop{hop_index}_{metric}"
    if ds_name not in ds_present:
        # avoid expensive exception path if DS is missing
        return ("skipped", f"{ip}_hop{hop_index}_{metric}_{label}.png", 0.0)

    png = png_path_hop(ip, hop_index, metric, label)
    if should_skip_png(png, rrd_path, ip):
        return ("skipped", png, time.time() - t0)

    safe_label = sanitize_label(hop_label)
    color = get_color_by_hop(hop_index)

    cmd = [
        f"DEF:v={rrd_path}:{ds_name}:AVERAGE",
        f"LINE1:v#{color}:{safe_label}",
        f"GPRINT:v:LAST:Last\\: %.1lf",
        f"GPRINT:v:MAX:Max\\: %.1lf",
        f"GPRINT:v:AVERAGE:Avg\\: %.1lf",
        "--units-exponent", "0",
        "--vertical-label", "Latency (ms)" if metric != "loss" else "Loss (%)",
        f"--title={ip} - Hop {hop_index} {metric.upper()} ({label})",
        f"--width={GRAPH_WIDTH}",
        f"--height={GRAPH_HEIGHT}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{seconds}",
    ]
    try:
        rrdtool.graph(png, *cmd)
        return ("ok", png, time.time() - t0)
    except rrdtool.OperationalError as e:
        logger.error(f"[ERROR] hop graph {ip} hop{hop_index} {metric} {label}: {e}")
        return ("error", png, time.time() - t0)

# ---- main loop ----
def main():
    # Load targets
    try:
        with open("mtr_targets.yaml") as f:
            targets = yaml.safe_load(f).get("targets", [])
    except Exception as e:
        logger.error(f"[ERROR] Failed to load mtr_targets.yaml: {e}")
        targets = []

    t_start = time.time()
    total, skipped, errors = 0, 0, 0

    jobs = []
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

        # Which files *should* exist (for cleanup)
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

        # Plan summary and hop jobs
        for metric in DATA_SOURCES:
            for rng in TIME_RANGES:
                label = rng.get("label")
                seconds = rng.get("seconds")
                if not label or not seconds:
                    continue
                jobs.append(("summary", (ip, rrd_path, metric, label, seconds, hops)))
                for hop_index, hop_label in hops:
                    jobs.append(("hop", (ip, rrd_path, hop_index, metric, label, seconds, ds_present, hop_label)))

    logger.info(f"Planned {len(jobs)} graph jobs (parallelism={PARALLELISM}, skip_unchanged={SKIP_UNCHANGED})")

    # Execute with small worker pool
    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futs = []
        for kind, args in jobs:
            if kind == "summary":
                futs.append(pool.submit(job_summary, *args))
            else:
                futs.append(pool.submit(job_hop, *args))

        for fut in as_completed(futs):
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
