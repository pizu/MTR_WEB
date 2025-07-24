# mtr_monitor.py
#
# Monitors a single IP using mtr --json, updates RRD, and logs changes.
# Patched with safe_float() and RRD debug logging.

import subprocess
import json
import time
import os
import yaml
import rrdtool
from datetime import datetime

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 'U'

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def run_mtr(target, source_ip=None):
    import shutil
    cmd = ["mtr", "--json", "--report-cycles", "1", target]
    if source_ip:
        cmd = ["mtr", "--json", "--report-cycles", "1", "--source", source_ip, target]

    print(f"[DEBUG] Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        print(f"[DEBUG] Return code: {result.returncode}")
        print(f"[DEBUG] STDOUT:\n{result.stdout.strip()}")
        print(f"[DEBUG] STDERR:\n{result.stderr.strip()}")

        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout)
        else:
            print(f"[ERROR] mtr failed for {target}")
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
            safe_float(h.get('avg')),
            safe_float(h.get('last')),
            safe_float(h.get('best')),
            safe_float(h.get('loss'))
        ]
    # Debug logging
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
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path = os.path.join(rrd_dir, f"{args.target}.rrd")
    interval = config.get("interval_seconds", 60)

    source_ip = None
    for t in yaml.safe_load(open("mtr_targets.yaml"))["targets"]:
        if t["ip"] == args.target:
            source_ip = t.get("source_ip")
            break

    print(f"[{datetime.now()}] Starting MTR for {args.target}")
    while True:
        result = run_mtr(args.target, source_ip)
        if result and "report" in result:
            update_rrd(rrd_path, result["report"], args.target)
        time.sleep(interval)

if __name__ == "__main__":
    main()
