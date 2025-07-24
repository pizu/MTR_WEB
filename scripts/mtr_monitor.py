# mtr_monitor.py
#
# This script monitors a target IP using MTR in JSON mode, continuously updating an RRD file
# with per-hop metrics (loss, avg, best, last) and logging key events.
#
# It is designed to be started by a controller script, passing target IP and source IP optionally.
#
# Version: 1.0

import subprocess
import time
import yaml
import rrdtool
import os
import signal
import logging
from datetime import datetime

# Global flag to indicate running state for clean shutdown
RUNNING = True

# Load a YAML file into a dictionary
def load_yaml(file):
    with open(file, 'r') as f:
        return yaml.safe_load(f)

# Set up a per-target logger, writing to a dedicated file in the log directory
def init_logger(target_ip, log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{target_ip}.log")
    logger = logging.getLogger(target_ip)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

# Create an RRD file for the target, if it doesn't exist already
def create_rrd(rrd_path, max_hops):
    if os.path.exists(rrd_path):
        return

    ds_defs = []
    for i in range(1, max_hops+1):
        for metric in ["avg", "last", "best", "loss"]:
            ds_defs.append(f"DS:hop{i}_{metric}:GAUGE:120:0:10000")
    rra_defs = ["RRA:AVERAGE:0.5:1:1440"]
    rrdtool.create(rrd_path, '--step', '60', *(ds_defs + rra_defs))

# Update the RRD with a new set of values for each hop
def update_rrd(rrd_path, hop_data, max_hops):
    values = []
    for i in range(max_hops):
        if i < len(hop_data):
            h = hop_data[i]
            values += [h.get('avg', 'U'), h.get('last', 'U'), h.get('best', 'U'), h.get('loss', 'U')]
        else:
            values += ['U', 'U', 'U', 'U']
    rrdtool.update(rrd_path, f"N:{':'.join(str(v) for v in values)}")

# Main MTR monitoring loop
def run_mtr_loop(target, settings):
    ip = target['ip']
    source = target.get('source_ip')
    interval = settings['interval_seconds']
    max_hops = settings['max_hops']
    rrd_dir = settings['rrd_directory']
    log_dir = settings['log_directory']

    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    os.makedirs(rrd_dir, exist_ok=True)
    create_rrd(rrd_path, max_hops)

    logger = init_logger(ip, log_dir)
    logger.info(f"Started monitoring {ip} (source: {source})")

    last_hosts = []
    while RUNNING:
        try:
            cmd = ["mtr", "--json", "-c", "10"]
            if source:
                cmd += ["-a", source]
            cmd.append(ip)

            result = subprocess.run(cmd, capture_output=True, text=True)
            data = yaml.safe_load(result.stdout)
            hops = data['report']['hubs']

            update_rrd(rrd_path, hops, max_hops)

            current_hosts = [h.get("host", "") for h in hops]
            if current_hosts != last_hosts:
                logger.info(f"Hop change detected: {current_hosts}")
                last_hosts = current_hosts

            for i, hop in enumerate(hops):
                if hop.get("loss", 0) > 0:
                    logger.info(f"Packet loss on hop {i+1}: {hop.get('loss')}%")

            time.sleep(interval)

        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
            time.sleep(interval)

    logger.info(f"Stopped monitoring {ip}")

# Handle termination signals for clean exit
def signal_handler(sig, frame):
    global RUNNING
    RUNNING = False

# Entry point
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="Target IP")
    parser.add_argument("--source", default=None, help="Optional source IP")
    parser.add_argument("--settings", default="mtr_script_settings.yaml")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    target = {"ip": args.target, "source_ip": args.source}
    settings = load_yaml(args.settings)
    run_mtr_loop(target, settings)
