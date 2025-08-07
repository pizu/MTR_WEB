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
import logging

# Load settings and logger
parser = argparse.ArgumentParser()
parser.add_argument("--settings", default="mtr_script_settings.yaml")
parser.add_argument("--target", required=True)
parser.add_argument("--source", help="Optional source IP for MTR")
args = parser.parse_args()

settings = load_settings(args.settings)
log_directory = settings.get("log_directory", "/tmp")
traceroute_dir = settings.get("traceroute_directory", "traceroute")
rrd_dir = settings.get("rrd_directory", "rrd")
max_hops = settings.get("max_hops", 30)
interval = settings.get("interval_seconds", 60)

logger = setup_logger("mtr_monitor", log_directory, "mtr_monitor.log", settings=settings, extra_file=f"{args.target}.log")

# Avoid duplicate handlers if restarting
if not logger.handlers:
    for h in shared_logger.handlers + target_logger.handlers:
        logger.addHandler(h)

# Update the RRD file with new metrics
def update_rrd(rrd_path, hops, ip, debug_log=None):
    values = []  # <-- Moved up here to ensure it's defined

    for i in range(0, max_hops + 1):
        hop = next((h for h in hops if h.get("count") == i), {})
        try:
            avg = round(float(hop.get("Avg", 'U')), 2) if hop.get("Avg") not in [None, 'U'] else 'U'
        except:
            avg = 'U'
        try:
            last = round(float(hop.get("Last", 'U')), 2) if hop.get("Last") not in [None, 'U'] else 'U'
        except:
            last = 'U'
        try:
            best = round(float(hop.get("Best", 'U')), 2) if hop.get("Best") not in [None, 'U'] else 'U'
        except:
            best = 'U'
        try:
            loss = round(float(hop.get("Loss%", 'U')) * 100, 2) if hop.get("Loss%") not in [None, 'U'] else 'U'
        except:
            loss = 'U'

        values += [avg, last, best, loss]

    timestamp = int(time.time())
    update_str = f"{timestamp}:" + ":".join(str(v) for v in values)

    try:
        rrdtool.update(rrd_path, update_str)
    except rrdtool.OperationalError as e:
        logger.error(f"[RRD ERROR] {e}")

    logger.debug(f"[{ip}] RRD Update values (first 8): {values[:8]}")

    if debug_log:
        with open(debug_log, "a") as f:
            f.write(f"{datetime.now()} {ip} values: {values}\n")


def init_rrd(rrd_path):
    if os.path.exists(rrd_path):
        return

    rrd_config = settings.get("rrd", {})
    step = rrd_config.get("step", 60)
    heartbeat = rrd_config.get("heartbeat", 120)
    ds_schema = rrd_config.get("data_sources", [])
    rra_schema = rrd_config.get("rras", [])

    data_sources = []
    for i in range(0, max_hops + 1):
        for ds in ds_schema:
            name = f"hop{i}_{ds['name']}"
            data_sources.append(f"DS:{name}:{ds['type']}:{heartbeat}:{ds['min']}:{ds['max']}")

    rras = [f"RRA:{r['cf']}:{r['xff']}:{r['step']}:{r['rows']}" for r in rra_schema]

    rrdtool.create(rrd_path, "--step", str(step), *data_sources, *rras)
    logger.info(f"[{rrd_path}] RRD created with dynamic schema from settings.")

def init_per_hop_rrds(ip):
    rrd_config = settings.get("rrd", {})
    step = rrd_config.get("step", 60)
    heartbeat = rrd_config.get("heartbeat", 120)
    ds_schema = rrd_config.get("data_sources", [])
    rra_schema = rrd_config.get("rras", [])

    os.makedirs(rrd_dir, exist_ok=True)

    for hop in range(max_hops + 1):
        hop_rrd_path = os.path.join(rrd_dir, f"{ip}_hop{hop}.rrd")
        if os.path.exists(hop_rrd_path):
            continue

        data_sources = []
        for ds in ds_schema:
            name = ds["name"]
            data_sources.append(f"DS:{name}:{ds['type']}:{heartbeat}:{ds['min']}:{ds['max']}")

        rras = [f"RRA:{r['cf']}:{r['xff']}:{r['step']}:{r['rows']}" for r in rra_schema]

        rrdtool.create(hop_rrd_path, "--step", str(step), *data_sources, *rras)
        logger.info(f"[{hop_rrd_path}] Per-hop RRD created.")

# Parse MTR JSON output (ignores hostname)
def parse_mtr_output(output):
    try:
        raw = json.loads(output)
        hops = raw["report"].get("hubs", [])
        for i, hop in enumerate(hops):
            hop["count"] = i  # Shift so first hop is hop0
            hop["host"] = hop.get("host", f"hop{i}")
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

# Save traceroute text and JSON map
def save_trace_and_json(ip, hops):
    os.makedirs(traceroute_dir, exist_ok=True)

    # Save plain text trace with hop number, IP, latency
    txt_path = os.path.join(traceroute_dir, f"{ip}.trace.txt")
    with open(txt_path, "w") as f:
        for hop in hops:
            hop_num = hop.get("count", "?")
            ip_addr = hop.get("host", "?")
            latency = hop.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")
    logger.info(f"Saved traceroute to {txt_path}")

    # Save JSON hop label map
    json_path = os.path.join(traceroute_dir, f"{ip}.json")
    hop_map = {f"hop{hop['count']}": hop.get("host", f"hop{hop['count']}") for hop in hops}
    with open(json_path, "w") as f:
        json.dump(hop_map, f, indent=2)
    logger.info(f"Saved hop label map to {json_path}")

# Compare hop paths for changes
def hops_changed(prev, curr):
    prev_hosts = [h.get("host") for h in prev]
    curr_hosts = [h.get("host") for h in curr]
    return prev_hosts != curr_hosts

# Main monitoring loop
def monitor_target(ip, source_ip=None):
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    init_rrd(rrd_path)
    init_per_hop_rrds(ip)
    
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    prev_hops = []
    logger.info(f"Starting monitoring for {ip}")
    while True:
        logger.info(f"Running MTR for {ip}")
        hops = run_mtr(ip, source_ip)

        if not hops:
            logger.warning(f"No data returned from MTR for {ip}")
            time.sleep(interval)
            continue

        if hops_changed(prev_hops, hops):
            prev_hosts = [h.get("host") for h in prev_hops]
            curr_hosts = [h.get("host") for h in hops]
            diff = DeepDiff(prev_hosts, curr_hosts, ignore_order=False)
            
            if diff:
                logger.info(f"{ip} hop path changed:")
                
                for key, value in diff.get("values_changed", {}).items():
                    hop_index = key.split("[")[-1].rstrip("]")
                    old = value.get("old_value")
                    new = value.get("new_value")
                    logger.info(f" - Hop {hop_index} changed from {old} to {new}")
                    
                for key, ip_added in diff.get("iterable_item_added", {}).items():
                    hop_index = key.split("[")[-1].rstrip("]")
                    logger.info(f" - Hop {hop_index} added: {ip_added}")
        
                for key, ip_removed in diff.get("iterable_item_removed", {}).items():
                    hop_index = key.split("[")[-1].rstrip("]")
                    logger.info(f" - Hop {hop_index} removed: {ip_removed}")
                    
            prev_hops = hops


        loss_hops = [h for h in hops if h.get("Loss%", 0) > 0]
        for hop in loss_hops:
            logger.warning(f"{ip} loss at hop {hop.get('count')}: {hop.get('Loss%')}%")
            
        logger.debug(f"[{ip}] Parsed hops: {[ (h.get('count'), h.get('host'), h.get('Avg')) for h in hops ]}")
        update_rrd(rrd_path, hops, ip, debug_rrd_log)
        save_trace_and_json(ip, hops)

        time.sleep(interval)

# Start monitoring
monitor_target(args.target, args.source)
