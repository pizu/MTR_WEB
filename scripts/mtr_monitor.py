#!/usr/bin/env python3
import subprocess
import time
import yaml
import json
import os
import logging
from datetime import datetime

# Load settings
with open("mtr_script_settings.yaml") as f:
    settings = yaml.safe_load(f)

with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

INTERVAL = settings.get("interval_seconds", 60)
LOG_DIR = settings.get("log_directory", "logs")
RRD_DIR = settings.get("rrd_directory", "data")
TRACEROUTE_DIR = "traceroute"
MAX_HOPS = settings.get("max_hops", 30)

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RRD_DIR, exist_ok=True)
os.makedirs(TRACEROUTE_DIR, exist_ok=True)

# Logging
logging.basicConfig(
    filename="mtr_master.log",
    format="%(asctime)s %(message)s",
    level=logging.INFO
)

# Save traceroute info
def save_traceroute(ip, hops):
    lines = []
    for hop in hops:
        if hop.get("host"):
            lines.append(hop["host"])
    path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    with open(path, "w") as f:
        f.write("\n".join(lines))

# Load previous traceroute (for hop change detection)
def load_previous_traceroute(ip):
    path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return f.read().splitlines()

# Compare hop paths
def detect_hop_changes(old, new):
    if old != new:
        return f"HOP CHANGE DETECTED\nOLD: {old}\nNEW: {new}"
    return None

# Detect loss events
def detect_loss(hops):
    loss_lines = []
    for hop in hops:
        if hop.get("Loss%") and hop["Loss%"] > 0:
            loss_lines.append(f"LOSS: {hop.get('count')}. {hop.get('host')} {hop['Loss%']}%")
    return loss_lines

# Main MTR loop
def run_mtr_loop():
    while True:
        for target in targets:
            ip = target["ip"]
            log_path = os.path.join(LOG_DIR, f"{ip}.log")
            try:
                cmd = ["mtr", "--json", "--report-cycles", "10", ip]
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                data = json.loads(proc.stdout)
                hops = data["report"]["hubs"]
                current_hop_path = [h.get("host", "???") for h in hops]

                # Save traceroute
                save_traceroute(ip, hops)

                # Detect hop changes
                old_path = load_previous_traceroute(ip)
                hop_change = detect_hop_changes(old_path, current_hop_path)

                # Detect loss
                loss_alerts = detect_loss(hops)

                # Log events
                with open(log_path, "a") as logf:
                    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
                    logf.write(f"{timestamp} MTR RUN\n")
                    if hop_change:
                        logf.write(f"{timestamp} {hop_change}\n")
                    for loss in loss_alerts:
                        logf.write(f"{timestamp} {loss}\n")
                    logf.write(f"{timestamp} HOPS: {current_hop_path}\n\n")

                logging.info(f"MTR complete for {ip}")

            except Exception as e:
                logging.error(f"Failed MTR for {ip}: {e}")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    logging.info("=== MTR Monitor Script STARTED ===")
    run_mtr_loop()
