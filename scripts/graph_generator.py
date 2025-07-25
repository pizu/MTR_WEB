#!/usr/bin/env python3
import os
import yaml
import rrdtool
from utils import load_settings, setup_logger

# Load settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("graph_generator", log_directory, "graph_generator.log")

RRD_DIR = settings.get("rrd_directory", "data")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
TRACEROUTE_DIR = "traceroute"
MAX_HOPS = settings.get("max_hops", 30)
GRAPH_WIDTH = settings.get("graph_width", 800)
GRAPH_HEIGHT = settings.get("graph_height", 200)
TIME_RANGES = settings.get("graph_time_ranges", ["1h", "6h", "12h", "24h", "1w"])

os.makedirs(GRAPH_DIR, exist_ok=True)

# Load target list
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

# Load traceroute hops
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
                hop_num = parts[0]
                host = parts[1]
                hops.append(f"{hop_num}: {host}")
            else:
                hops.append(f"{len(hops)+1}: (unknown)")
    return hops

# Generate a graph for an IP/metric/time range
def generate_graph(ip, metric, timerange):
    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    png_path = os.path.join(GRAPH_DIR, f"{ip}_{metric}_{timerange}.png")

    if not os.path.exists(rrd_path):
        logger.warning(f"[SKIP] No RRD for {ip}")
        return

    traceroute_labels = get_labels(ip)
    defs = []
    lines = []

    for i in range(1, MAX_HOPS + 1):
        ds_name = f"hop{i}_{metric}"
        label = traceroute_labels[i - 1] if i - 1 < len(traceroute_labels) else f"Hop{i}"
        color = f"{(i * 73 % 256):02x}{(i * 137 % 256):02x}{(255 - i * 47 % 256):02x}"
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{color}:{label}")

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()} ({timerange})",
        f"--width={GRAPH_WIDTH}",
        f"--height={GRAPH_HEIGHT}",
        "--slope-mode",
        "--end", "now",
        f"--start=-{timerange}"
    ]

    try:
        rrdtool.graph(png_path, *cmd)
        logger.info(f"[GRAPHED] {png_path}")
    except rrdtool.OperationalError as e:
        logger.error(f"[ERROR] {ip} - {metric} ({timerange}): {e}")

# Generate all graphs
for target in targets:
    ip = target["ip"]
    for metric in ["avg", "last", "best", "loss"]:
        for rng in TIME_RANGES:
            generate_graph(ip, metric, rng)
