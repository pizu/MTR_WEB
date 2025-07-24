# mtr_monitor.py
#
# Monitors a single IP using mtr --json, updates RRD, and logs changes.
# Patched with safe_float(), RRD debug logging, JSON key normalization, and per-IP logs.

import subprocess
import json
import time
import os
import yaml
import rrdtool
import logging
from datetime import datetime

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 'U'

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def init_logger(ip, log_dir):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(ip)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(os.path.join(log_dir, f"{ip}.log"))
    formatter = logging.Formatter('%(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def run_mtr(target, source_ip=None):
    cmd = ["mtr", "--json", "--report-cycles", "1", target]
    if source_ip:
        cmd = ["mtr", "--json", "--report-cycles", "1", "--source", source_ip, target]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout)
        else:
            print(f"[ERROR] mtr failed for {target}")
            print("STDOUT:", result.stdout.strip())
            print("STDERR:", result.stderr.strip())
            return None
    except Exception as e:
        print(f"[EXCEPTION] mtr failed for {target}: {e}")
        return None

def ensure_rrd(rrd_path):
    if not os.path.exists(rrd_path):
        ds_list = []
        for i in range(1, 31):
            for metric in ['avg', 'last', 'best', 'loss']:
                ds_list.append(f"DS:hop{i}_{metric}:GAUGE:600:0:U")
        rrdtool.create(
            rrd_path,
            "--step", "60",
            *(ds_list + ["RRA:AVERAGE:0.5:1:1440"])
        )

def update_rrd(rrd_path, hops, ip):
    ensure_rrd(rrd_path)
    values = []
    for i in range(30):
        try:
            h = hops[i]
        except IndexError:
            h = {}
        values += [
            safe_float(h.get('Avg')),
            safe_float(h.get('Last')),
            safe_float(h.get('Best')),
            safe_float(h.get('Loss%'))
        ]
    with open("rrd_debug.log", "a") as dbg:
        dbg.write(f"{datetime.now()} {ip} values: {values}\n")
    rrdtool.update(rrd_path, f"N:{':'.join(str(v) for v in values)}")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--settings", default="mtr_script_settings.yaml")
    args = parser.parse_args()

    config = load_config(args.settings)
    rrd_dir = config["rrd_directory"]
    log_dir = config["log_directory"]
    os.makedirs(rrd_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    rrd_path = os.path.join(rrd_dir, f"{args.target}.rrd")
    interval = config.get("interval_seconds", 60)

    logger = init_logger(args.target, log_dir)
    logger.info("Started MTR monitoring")

    source_ip = None
    for t in yaml.safe_load(open("mtr_targets.yaml"))["targets"]:
        if t["ip"] == args.target:
            source_ip = t.get("source_ip")
            break

    while True:
        result = run_mtr(args.target, source_ip)
        if result:
            print(f"[DEBUG] MTR JSON result = {json.dumps(result, indent=2)}")
            hops = result.get("report", {}).get("hubs", [])
            if hops:
                update_rrd(rrd_path, hops, args.target)
                logger.info(f"Updated RRD with {len(hops)} hops")
            else:
                logger.warning("No valid hop data found in JSON")
        else:
            logger.warning("No result from mtr")

        time.sleep(interval)

if __name__ == "__main__":
    main()
