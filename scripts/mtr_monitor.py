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

# -------------------------------
# Load settings and CLI arguments
# -------------------------------
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
severity_rules = settings.get("log_severity_rules", [])

logger = setup_logger("mtr_monitor", log_directory, "mtr_monitor.log", settings=settings, extra_file=f"{args.target}.log")

# -------------------------------
# Optional YAML-based severity tagging
# -------------------------------
def evaluate_severity_rules(rules, context):
    if not rules:
        return None, None
    for rule in rules:
        try:
            if eval(rule["match"], {}, context):
                return rule["tag"], rule["level"]
        except Exception as e:
            logger.debug(f"[SEVERITY_RULE_ERROR] {e} — Rule: {rule}")
    return None, None

# -------------------------------
# RRD handling
# -------------------------------
def update_rrd(rrd_path, hops, ip, debug_log=None):
    values = []
    for i in range(0, max_hops + 1):
        hop = next((h for h in hops if h.get("count") == i), {})
        try: avg = round(float(hop.get("Avg", 'U')), 2) if hop.get("Avg") not in [None, 'U'] else 'U'
        except: avg = 'U'
        try: last = round(float(hop.get("Last", 'U')), 2) if hop.get("Last") not in [None, 'U'] else 'U'
        except: last = 'U'
        try: best = round(float(hop.get("Best", 'U')), 2) if hop.get("Best") not in [None, 'U'] else 'U'
        except: best = 'U'
        try: loss = round(float(hop.get("Loss%", 'U')) * 100, 2) if hop.get("Loss%") not in [None, 'U'] else 'U'
        except: loss = 'U'
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
            f.write(f"{datetime.now()} {ip} values: {values}
")


def update_per_hop_rrds(ip, hops):
    for hop in hops:
        hop_index = hop.get("count")
        if hop_index is None:
            continue
        rrd_file = os.path.join(rrd_dir, f"{ip}_hop{hop_index}.rrd")
        if not os.path.exists(rrd_file):
            continue
        try:
            update_str = f"N:"
            update_str += ":".join(str(round(float(hop.get(ds['name'], 'U')), 2)) if hop.get(ds['name']) not in [None, 'U'] else 'U'
                                   for ds in settings.get("rrd", {}).get("data_sources", []))
            rrdtool.update(rrd_file, update_str)
        except Exception as e:
            logger.warning(f"[{ip}] Failed to update {rrd_file}: {e}")

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
        data_sources = [f"DS:{ds['name']}:{ds['type']}:{heartbeat}:{ds['min']}:{ds['max']}" for ds in ds_schema]
        rras = [f"RRA:{r['cf']}:{r['xff']}:{r['step']}:{r['rows']}" for r in rra_schema]
        rrdtool.create(hop_rrd_path, "--step", str(step), *data_sources, *rras)
        logger.info(f"[{hop_rrd_path}] Per-hop RRD created.")

# -------------------------------
# MTR handling
# -------------------------------
def parse_mtr_output(output):
    try:
        raw = json.loads(output)
        hops = raw["report"].get("hubs", [])
        for i, hop in enumerate(hops):
            hop["count"] = i
            hop["host"] = hop.get("host", f"hop{i}")
        return hops
    except Exception as e:
        logger.error(f"[PARSE ERROR] {e}")
        return []

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

def save_trace_and_json(ip, hops):
update_per_hop_rrds(ip, hops)
    os.makedirs(traceroute_dir, exist_ok=True)
    txt_path = os.path.join(traceroute_dir, f"{ip}.trace.txt")
    with open(txt_path, "w") as f:
        for hop in hops:
            hop_num = hop.get("count", "?")
            ip_addr = hop.get("host", "?")
            latency = hop.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms
")
    logger.info(f"Saved traceroute to {txt_path}")
    json_path = os.path.join(traceroute_dir, f"{ip}.json")
    hop_map = {f"hop{hop['count']}": hop.get("host", f"hop{hop['count']}") for hop in hops}
    with open(json_path, "w") as f:
        json.dump(hop_map, f, indent=2)
    logger.info(f"Saved hop label map to {json_path}")

def hops_changed(prev, curr):
    prev_hosts = [h.get("host") for h in prev]
    curr_hosts = [h.get("host") for h in curr]
    return prev_hosts != curr_hosts

# -------------------------------
# Main monitoring loop
# -------------------------------
def monitor_target(ip, source_ip=None):
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    init_rrd(rrd_path)
    init_per_hop_rrds(ip)
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")
    prev_hops = []
    prev_loss_state = {}

    logger.info(f"[{ip}] Monitoring loop started — running MTR")

    while True:
        hops = run_mtr(ip, source_ip)
        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — target unreachable or command failed")
            time.sleep(interval)
            continue

        curr_hosts = [h.get("host") for h in hops]
        hop_path_changed = hops_changed(prev_hops, hops)
        curr_loss_state = {h.get("count"): round(h.get("Loss%", 0), 2) for h in hops if h.get("Loss%", 0) > 0}
        loss_changed = curr_loss_state != prev_loss_state

        if hop_path_changed:
            diff = DeepDiff([h.get("host") for h in prev_hops], curr_hosts, ignore_order=False)
            context = {
                "hop_changed": True,
                "hop_added": bool(diff.get("iterable_item_added")),
                "hop_removed": bool(diff.get("iterable_item_removed")),
            }
            for key, value in diff.get("values_changed", {}).items():
                hop_index = key.split("[")[-1].rstrip("]")
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, level.lower(), logger.info) if tag and level else logger.info
                msg = f"[{ip}] Hop {hop_index} changed from {value.get('old_value')} to {value.get('new_value')}"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        if loss_changed:
            for hop_num, loss in curr_loss_state.items():
                context = {
                    "loss": loss,
                    "prev_loss": prev_loss_state.get(hop_num, 0),
                    "hop_changed": hop_path_changed,
                }
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, level.lower(), logger.warning if loss > 0 else logger.info) if isinstance(level, str) else (logger.warning if loss > 0 else logger.info)
                msg = f"[{ip}] Loss at hop {hop_num}: {loss}% (prev: {context['prev_loss']}%)"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        if hop_path_changed or loss_changed:
            logger.debug(f"[{ip}] Parsed hops: {[ (h.get('count'), h.get('host'), h.get('Avg')) for h in hops ]}")
            update_rrd(rrd_path, hops, ip, debug_rrd_log)
            save_trace_and_json(ip, hops)
            update_per_hop_rrds(ip, hops)
            logger.info(f"[{ip}] Traceroute and hop map saved.")
        else:
            logger.debug(f"[{ip}] No change detected — {len(hops)} hops parsed. No update performed.")

        prev_hops = hops
        prev_loss_state = curr_loss_state
        time.sleep(interval)

monitor_target(args.target, args.source)
