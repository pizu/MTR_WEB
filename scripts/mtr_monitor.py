#!/usr/bin/env python3
import os
import sys
import time
import yaml
import json
import rrdtool
import subprocess
from datetime import datetime
from pathlib import Path
import argparse
from deepdiff import DeepDiff

# Load YAML settings
def load_settings(path):
    with open(path) as f:
        return yaml.safe_load(f)

# Update the RRD file with new metrics
def update_rrd(rrd_path, hops, ip, debug_log=None):
    values = []
    for i in range(1, settings.get("max_hops", 30) + 1):
        hop = next((h for h in hops if h.get("count") == i), {})
        values += [
            hop.get("Avg", 'U'),
            hop.get("Last", 'U'),
            hop.get("Best", 'U'),
            hop.get("Loss%", 'U')
        ]

    timestamp = int(time.time())
    update_str = f"{timestamp}:" + ":".join(str(v) for v in values)
    try:
        rrdtool.update(rrd_path, update_str)
    except rrdtool.OperationalError as e:
        print(f"[RRD ERROR] {e}")

    if debug_log:
        with open(debug_log, "a") as f:
            f.write(f"{datetime.now()} {ip} values: {values}\n")

# Initialize RRD if not exists
def init_rrd(rrd_path, max_hops):
    if os.path.exists(rrd_path):
        return
    data_sources = []
    for i in range(1, max_hops + 1):
        for metric in ["avg", "last", "best", "loss"]:
            data_sources.append(f"DS:hop{i}_{metric}:GAUGE:120:0:1000000")
    rrdtool.create(
        rrd_path,
        "--step", "60",
        *data_sources,
        "RRA:AVERAGE:0.5:1:1440"
    )

# Parse MTR JSON output (ignores hostname)
def parse_mtr_output(output):
    try:
        raw = json.loads(output)
        hops = raw["report"].get("hubs", [])
        for hop in hops:
            hop["host"] = hop.get("host", f"hop{hop['count']}")
        return hops
    except Exception as e:
        print(f"[PARSE ERROR] {e}")
        return []

# Run MTR with given source (if any)
def run_mtr(target, source_ip=None):
    cmd = ["mtr", "--json", "--report-cycles", "1", "--no-dns"]
    if source_ip:
        cmd += ["--source", source_ip]
    cmd.append(target)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if result.returncode == 0:
            return parse_mtr_output(result.stdout)
        else:
            print(f"[MTR ERROR] {result.stderr}")
            return []
    except Exception as e:
        print(f"[EXCEPTION] MTR run failed: {e}")
        return []

# Log helper
def log_event(ip, message):
    log_file = os.path.join(settings["log_directory"], f"{ip}.log")
    with open(log_file, "a") as f:
        f.write(f"[{datetime.now()}] {message}\n")

# Save traceroute to text
def save_trace(ip, hops):
    path = os.path.join("traceroute", f"{ip}.trace.txt")
    with open(path, "w") as f:
        for hop in hops:
            f.write(f"{hop.get('host', '???')}\n")

# Compare hop paths for changes
def hops_changed(prev, curr):
    prev_hosts = [h.get("host") for h in prev]
    curr_hosts = [h.get("host") for h in curr]
    return prev_hosts != curr_hosts

# Main loop for target
def monitor_target(ip, source_ip=None):
    rrd_path = os.path.join(settings["rrd_directory"], f"{ip}.rrd")
    init_rrd(rrd_path, settings.get("max_hops", 30))
    debug_rrd_log = "rrd_debug.log"

    prev_hops = []
    while True:
        log_event(ip, "MTR RUN")
        hops = run_mtr(ip, source_ip)

        if not hops:
            log_event(ip, "No data returned from MTR")
            time.sleep(settings["interval_seconds"])
            continue

        if hops_changed(prev_hops, hops):
            diff = DeepDiff(
                [h.get("host") for h in prev_hops],
                [h.get("host") for h in hops],
                ignore_order=True
            )
            log_event(ip, f"Hop path changed: {diff.pretty()}")
            prev_hops = hops

        loss_hops = [h for h in hops if h.get("Loss%", 0) > 0]
        for hop in loss_hops:
            log_event(ip, f"Loss at hop {hop.get('count')}: {hop.get('Loss%')}%")

        update_rrd(rrd_path, hops, ip, debug_rrd_log)
        save_trace(ip, hops)

        time.sleep(settings["interval_seconds"])

# Entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--settings", default="mtr_script_settings.yaml")
    parser.add_argument("--target", required=True)
    parser.add_argument("--source", help="Optional source IP for MTR")
    args = parser.parse_args()

    settings = load_settings(args.settings)
    monitor_target(args.target, args.source)
