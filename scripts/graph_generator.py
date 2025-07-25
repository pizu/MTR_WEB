#!/usr/bin/env python3
import os
import yaml
import rrdtool
from utils import load_settings, setup_logger

# Load settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("graph_generator", log_directory, "graph_generator.log")

# Configuration paths
RRD_DIR = settings.get("rrd_directory", "data")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
TRACEROUTE_DIR = "traceroute"
MAX_HOPS = settings.get("max_hops", 30)
GRAPH_WIDTH = settings.get("graph_width", 800)
GRAPH_HEIGHT = settings.get("graph_height", 200)

os.makedirs(GRAPH_DIR, exist_ok=True)

# Load targets
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

# Helper to load hop labels from traceroute file
def get_labels(ip):
    path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    if os.path.exists(path):
        with open(path) as f:
            return [line.strip() for line in f if line.strip()]
    return []

# Generate a single graph for one metric
def generate_graph(ip, metric):
    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    png_path = os.path.join(GRAPH_DIR, f"{ip}_{metric}.png")

    if not os.path.exists(rrd_path):
        logger.warning(f"[SKIP] No RRD file found for {ip}")
        return

    traceroute = get_labels(ip)
    defs = []
    lines = []

    for i in range(1, MAX_HOPS + 1):
        ds_name = f"hop{i}_{metric}"
        label = traceroute[i - 1] if i - 1 < len(traceroute) else f"Hop{i} (unknown)"
        color = f"{(i * 47 % 256):02x}{(255 - i * 29 % 256):02x}{(i * 71 % 256):02x}"
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{color}:{label}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} per hop",
        f"--width={GRAPH_WIDTH}",
        f"--height={GRAPH_HEIGHT}",
        "--slope-mode",
        "--end", "now",
        "--start", "-1h"
    ]

    try:
        rrdtool.graph(png_path, *cmd)
        logger.info(f"[GRAPHED] {png_path}")
    except rrdtool.OperationalError as e:
        logger.error(f"[ERROR] Failed to graph {ip} ({metric}): {e}")

# Main loop to generate all graphs
for target in targets:
    ip = target["ip"]
    for metric in ["avg", "last", "best", "loss"]:
        generate_graph(ip, metric)
