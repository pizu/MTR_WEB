#!/usr/bin/env python3
#
# controller.py
#
# This script watches mtr_targets.yaml and manages child mtr_monitor.py processes per target.
# It starts/stops monitor processes based on target config changes.
#
# Usage: python3 controller.py
# Ensure mtr_monitor.py and the YAML files are in place.

import yaml
import time
import subprocess
import threading
from utils import load_settings, setup_logger

# Initialize settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("controller", log_directory, "controller.log")

CONFIG_FILE = "mtr_targets.yaml"
SCRIPT_SETTINGS_FILE = "mtr_script_settings.yaml"
MONITOR_SCRIPT = "scripts/mtr_monitor.py"

# Track running processes per target
monitored_targets = {}
lock = threading.Lock()

def load_targets():
    """
    Load current targets from the mtr_targets.yaml file.
    """
    try:
        with open(CONFIG_FILE, "r") as f:
            data = yaml.safe_load(f)
            return data.get("targets", [])
    except Exception as e:
        logger.error(f"Failed to load targets: {e}")
        return []

def start_monitor(target):
    """
    Start the mtr_monitor.py script for a given target.
    """
    try:
        args = [
            "python3", MONITOR_SCRIPT,
            "--target", target["ip"],
            "--settings", SCRIPT_SETTINGS_FILE
        ]
        if target.get("source_ip"):
            args += ["--source", target["source_ip"]]

        proc = subprocess.Popen(args)
        logger.info(f"Started monitor for {target['ip']} (PID: {proc.pid})")
        return proc
    except Exception as e:
        logger.error(f"Failed to start monitor for {target['ip']}: {e}")
        return None

def stop_monitor(ip):
    """
    Stop the monitoring process for a specific IP.
    """
    try:
        with lock:
            proc = monitored_targets.pop(ip, None)
            if proc:
                proc.terminate()
                logger.info(f"Stopped monitor for {ip}")
    except Exception as e:
        logger.error(f"Failed to stop monitor for {ip}: {e}")

def stop_all():
    """
    Stop all running monitor processes.
    """
    with lock:
        for ip, proc in monitored_targets.items():
            try:
                proc.terminate()
                logger.info(f"Terminated monitor for {ip}")
            except Exception as e:
                logger.error(f"Error terminating process for {ip}: {e}")
        monitored_targets.clear()

def monitor_loop():
    """
    Continuously watch the targets file and start/stop monitors as needed.
    """
    logger.info("Controller started. Watching targets...")
    current_targets = []

    while True:
        try:
            new_targets = load_targets()
            new_ips = {t["ip"] for t in new_targets}
            cur_ips = set(monitored_targets.keys())

            # Start new monitors
            for target in new_targets:
                ip = target["ip"]
                if ip not in cur_ips:
                    proc = start_monitor(target)
                    if proc:
                        monitored_targets[ip] = proc

            # Stop monitors for removed targets
            for ip in cur_ips - new_ips:
                stop_monitor(ip)

            time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt. Shutting down...")
            stop_all()
            break
        except Exception as e:
            logger.error(f"Error in monitor loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    monitor_loop()
