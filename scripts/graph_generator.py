#!/usr/bin/env python3
import os
import yaml
import rrdtool
import re
import math
from utils import load_settings, setup_logger

# Load settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("graph_generator", log_directory, "graph_generator.log")

RRD_DIR = settings.get("rrd_directory", "data")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
TRACEROUTE_DIR = settings.get("traceroute_directory", "traceroute")
MAX_HOPS = settings.get("max_hops", 30)
GRAPH_WIDTH = settings.get("graph_width", 800)
GRAPH_HEIGHT = settings.get("graph_height", 200)
TIME_RANGES = settings.get("graph_time_ranges", [])

os.makedirs(GRAPH_DIR, exist_ok=True)

# Sanitize label for RRDTool and file naming
def sanitize_label(label):
    return re.sub(r'[:\\\'"]', '-', label)

# Assign distinct colors per hop using sine waves
def get_color_by_hop(hop_index):
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"{r:02x}{g:02x}{b:02x}"

# Load traceroute hop labels
def get_labels(ip):
    path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    if not os.path.exists(path):
        return []

    hops = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=2)
            if len(parts) >= 2:
                try:
                    hop_num = int(parts[0])
                    hop_ip = parts[1]
                    hops.append((hop_num, f"Hop {hop_num} - {hop_ip}"))
                except ValueError:
                    continue
    return hops

# Remove obsolete graphs for this IP
def clean_old_graphs(ip, expected_pngs):
    for fname in os.listdir(GRAPH_DIR):
        if fname.startswith(f"{ip}_") and fname.endswith(".png"):
            full_path = os.path.join(GRAPH_DIR, fname)
            if fname not in expected_pngs:
                try:
                    os.remove(full_path)
                    logger.info(f"[CLEANED] {fname}")
                except Exception as e:
                    logger.warning(f"[SKIP CLEANUP] {fname}: {e}")

# Generate one graph
def generate_graph(ip, metric, timerange_label, timerange_seconds, hops):
    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    png_filename = f"{ip}_{metric}_{timerange_label}.png"
    png_path = os.path.join(GRAPH_DIR, png_filename)

    if not os.path.exists(rrd_path):
        logger.warning(f"[SKIP] No RRD for {ip}")
        return

    defs = []
    lines = []

    for hop_index, raw_label in hops:
        if hop_index > MAX_HOPS:
            continue
        ds_name = f"hop{hop_index}_{metric}"
        safe_label = sanitize_label(raw_label)
        color = get_color_by_hop(hop_index)
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{color}:{safe_label}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({timerange_label})",
        f"--width={GRAPH_WIDTH}",
        f"--height={GRAPH_HEIGHT}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{timerange_seconds}"
    ]

    try:
        rrdtool.graph(png_path, *cmd)
        logger.info(f"[GRAPHED] {png_path}")
    except rrdtool.OperationalError as e:
        logger.error(f"[ERROR] {ip} - {metric} ({timerange_label}): {e}")

# Main process
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f).get("targets", [])
except Exception as e:
    logger.error(f"[ERROR] Failed to load mtr_targets.yaml: {e}")
    targets = []

for target in targets:
    ip = target.get("ip")
    if not ip:
        continue

    hops = get_labels(ip)
    if not hops:
        logger.warning(f"[SKIP] No valid traceroute data for {ip}")
        continue

    # Build expected graph filenames
    expected_pngs = []
    for metric in ["avg", "last", "best", "loss"]:
        for range_def in TIME_RANGES:
            label = range_def.get("label")
            if label:
                expected_pngs.append(f"{ip}_{metric}_{label}.png")

    clean_old_graphs(ip, expected_pngs)

    for metric in ["avg", "last", "best", "loss"]:
        for range_def in TIME_RANGES:
            label = range_def.get("label")
            seconds = range_def.get("seconds")
            if label and seconds:
                generate_graph(ip, metric, label, seconds, hops)
