#!/usr/bin/env python3
import os
import yaml
import rrdtool
import re
from utils import load_settings, setup_logger

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

# Sanitize label to avoid RRDTool errors
def sanitize_label(label):
    return re.sub(r'[:\\\'"]', '-', label)

# Determine color based on RTT
def get_rtt_color(latency_ms):
    try:
        rtt = float(latency_ms.replace("ms", "").strip())
        if rtt > 150:
            return "ff0000"  # Red
        elif rtt > 50:
            return "ffa500"  # Orange
        else:
            return "00cc00"  # Green
    except:
        return "00cc00"  # Default to green on parse failure

# Load traceroute hops with RTT info
def get_labels_and_rtt(ip):
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
            if len(parts) >= 3:
                hop_num, hop_ip, latency = parts[0], parts[1], parts[2]
                hops.append((int(hop_num), f"Hop {hop_num} - {hop_ip}", latency))
            elif len(parts) == 2:
                hop_num, hop_ip = parts[0], parts[1]
                hops.append((int(hop_num), f"Hop {hop_num} - {hop_ip}", "0 ms"))
            else:
                hops.append((len(hops), f"Hop {len(hops)} - unknown", "0 ms"))
    return hops

# Clean old graphs
def clean_old_graphs(ip):
    for fname in os.listdir(GRAPH_DIR):
        if fname.startswith(f"{ip}_") and fname.endswith(".png"):
            try:
                os.remove(os.path.join(GRAPH_DIR, fname))
                logger.info(f"[CLEANED] {fname}")
            except Exception as e:
                logger.warning(f"[SKIP CLEANUP] Could not remove {fname}: {e}")

# Generate graph
def generate_graph(ip, metric, timerange):
    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    png_path = os.path.join(GRAPH_DIR, f"{ip}_{metric}_{timerange}.png")

    if not os.path.exists(rrd_path):
        logger.warning(f"[SKIP] No RRD for {ip}")
        return

    hops = get_labels_and_rtt(ip)
    if not hops:
        logger.warning(f"[SKIP] No traceroute data for {ip}")
        return

    # Limit to max_hops
    hops = sorted(hops, key=lambda h: h[0])
    hops = [h for h in hops if h[0] <= MAX_HOPS]

    defs = []
    lines = []

    for hop in hops:
        hop_index, raw_label, latency = hop
        ds_name = f"hop{hop_index}_{metric}"
        safe_label = sanitize_label(raw_label)
        color = get_rtt_color(latency)
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{color}:{safe_label}")

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

# Main generation loop
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

for target in targets:
    ip = target["ip"]
    clean_old_graphs(ip)
    for metric in ["avg", "last", "best", "loss"]:
        for rng in TIME_RANGES:
            generate_graph(ip, metric, rng)
