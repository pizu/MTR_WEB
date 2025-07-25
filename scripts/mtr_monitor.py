#!/usr/bin/env python3
import os
import sys
import time
import json
import rrdtool
import subprocess
from datetime import datetime
from pathlib import Path
import argparse
from deepdiff import DeepDiff
from utils import load_settings, setup_logger

# Load settings and logger
parser = argparse.ArgumentParser()
parser.add_argument("--settings", default="mtr_script_settings.yaml")
parser.add_argument("--target", required=True)
parser.add_argument("--source", help="Optional source IP for MTR")
args = parser.parse_args()

settings = load_settings(args.settings)
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("mtr_monitor", log_directory, "mtr_monitor.log")

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
        logger.error(f"[RRD ERROR] {e}")

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
    logger.info(f"Initialized RRD at {rrd_path}")

# Parse MTR JSON output (ignores hostname)
def parse_mtr_output(output):
    try:
        raw = json.loads(output)
        hops = raw["report"].get("hubs", [])
        for hop in hops:
            hop["host"] = hop.get("host", f"hop{hop['count']}")
        return hops
    except Exception as e:
        logger.error(f"[PARSE ERROR] {e}")
        return []

# Run MTR with given source (if any)
def run_mtr(target, source_ip=None):
    cmd = ["mtr", "--json", "--report-cycles", "1", "--no-dns"]
    if source_ip:
        cmd += ["--address", source_ip]
    cmd.append(target)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if result.returncode == 0:
            return parse_mtr_output(result.stdout)
        else:
            logger.error(f"[MTR ERROR] {result.stderr.strip()}")
            return []
    except Exception as e:
        logger.exception(f"[EXCEPTION] MTR run failed: {e}")
        return []

# Save traceroute to file
def save_trace(ip, hops):
    traceroute_dir = settings.get("traceroute_directory", "traceroute")
    os.makedirs(traceroute_dir, exist_ok=True)
    path = os.path.join(traceroute_dir, f"{ip}.trace.txt")
    with open(path, "w") as f:
        for hop in hops:
            f.write(f"{hop.get('host', '???')}\n")
    logger.info(f"Saved traceroute to {path}")

# Compare hop paths for changes
def hops_changed(prev, curr):
    prev_hosts = [h.get("host") for h in prev]
    curr_hosts = [h.get("host") for h in curr]
    return prev_hosts != curr_hosts

# Main monitoring loop
def monitor_target(ip, source_ip=None):
    rrd_path = os.path.join(settings["rrd_directory"], f"{ip}.rrd")
    init_rrd(rrd_path, settings.get("max_hops", 30))
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    prev_hops = []
    logger.info(f"Starting monitoring for {ip}")
    while True:
        logger.info(f"Running MTR for {ip}")
        hops = run_mtr(ip, source_ip)

        if not hops:
            logger.warning(f"No data returned from MTR for {ip}")
            time.sleep(settings["interval_seconds"])
            continue

        if hops_changed(prev_hops, hops):
            diff = DeepDiff(
                [h.get("host") for h in prev_hops],
                [h.get("host") for h in hops],
                ignore_order=True
            )
            logger.info(f"{ip} hop path changed: {diff.pretty()}")
            prev_hops = hops

        loss_hops = [h for h in hops if h.get("Loss%", 0) > 0]
        for hop in loss_hops:
            logger.warning(f"{ip} loss at hop {hop.get('count')}: {hop.get('Loss%')}%")

        update_rrd(rrd_path, hops, ip, debug_rrd_log)
        save_trace(ip, hops)

        time.sleep(settings["interval_seconds"])

# Start monitoring
monitor_target(args.target, args.source)
