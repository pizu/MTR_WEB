#!/usr/bin/env python3
import os
import yaml
import rrdtool

# Load settings
with open("mtr_script_settings.yaml") as f:
    settings = yaml.safe_load(f)

RRD_DIR = settings.get("rrd_directory", "data")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
TRACEROUTE_DIR = "traceroute"
MAX_HOPS = settings.get("max_hops", 30)

os.makedirs(GRAPH_DIR, exist_ok=True)

# Load targets
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

def get_labels(ip):
    path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    if os.path.exists(path):
        with open(path) as f:
            return [line.strip() for line in f if line.strip()]
    return []

def generate_graph(ip, metric):
    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    png_path = os.path.join(GRAPH_DIR, f"{ip}_{metric}.png")

    if not os.path.exists(rrd_path):
        print(f"[SKIP] No RRD for {ip}")
        return

    traceroute = get_labels(ip)
    defs = []
    lines = []
    hop_labels = []

    for i in range(1, MAX_HOPS + 1):
        ds_name = f"hop{i}_{metric}"
        label = traceroute[i - 1] if i - 1 < len(traceroute) else f"Hop{i}"
        color = f"{(i*50 % 255):02x}00{(255 - i*30 % 255):02x}"
        defs.append(f"DEF:{ds_name}={rrd_path}:{ds_name}:AVERAGE")
        lines.append(f"LINE1:{ds_name}#{color}:{label}")
        hop_labels.append(label)

    cmd = defs + lines + [
        f"--title={ip} - {metric.upper()}",
        "--width", "800",
        "--height", "200",
        "--slope-mode",
        "--end", "now",
        "--start", "-1h"
    ]

    try:
        rrdtool.graph(png_path, *cmd)
        print(f"[GRAPHED] {png_path}")
    except rrdtool.OperationalError as e:
        print(f"[ERROR] {e}")

# Generate graphs
for target in targets:
    ip = target["ip"]
    for metric in ["avg", "last", "best", "loss"]:
        generate_graph(ip, metric)
