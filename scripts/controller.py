#!/usr/bin/env python3
#
# controller.py
#
# Watches mtr_targets.yaml and mtr_script_settings.yaml.
# Starts/stops mtr_watchdog.py processes based on target or settings changes.

import yaml
import time
import subprocess
import threading
import os
from utils import load_settings, setup_logger

# Constants
CONFIG_FILE = "mtr_targets.yaml"
SCRIPT_SETTINGS_FILE = "mtr_script_settings.yaml"
MONITOR_SCRIPT = "scripts/mtr_watchdog.py"  # <== updated to match new entrypoint

# Globals
settings = load_settings(SCRIPT_SETTINGS_FILE)
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("controller", log_directory, "controller.log", settings=settings)

monitored_targets = {}
lock = threading.Lock()
last_settings_mtime = os.path.getmtime(SCRIPT_SETTINGS_FILE)

def load_targets():
    """
    Reads the YAML file with the list of targets.
    Returns a list of target dictionaries with 'ip' and optionally 'source_ip'.
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
    Starts a new subprocess for a single target using mtr_watchdog.py.
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
    Stops the subprocess for a specific target IP.
    """
    with lock:
        proc = monitored_targets.pop(ip, None)
        if proc:
            try:
                proc.terminate()
                logger.info(f"Stopped monitor for {ip}")
            except Exception as e:
                logger.error(f"Failed to stop monitor for {ip}: {e}")

def stop_all():
    """
    Stops all running monitor subprocesses.
    """
    with lock:
        for ip, proc in monitored_targets.items():
            try:
                proc.terminate()
                logger.info(f"Terminated monitor for {ip}")
            except Exception as e:
                logger.error(f"Error terminating process for {ip}: {e}")
        monitored_targets.clear()

def restart_all_monitors(targets):
    """
    Restarts all monitor subprocesses — used when settings.yaml is updated.
    """
    logger.info("Settings changed. Restarting all monitors.")
    stop_all()
    time.sleep(1)
    for target in targets:
        proc = start_monitor(target)
        if proc:
            monitored_targets[target["ip"]] = proc

def monitor_loop():
    """
    Main controller loop that checks for changes to:
    - mtr_script_settings.yaml (to trigger full restarts)
    - mtr_targets.yaml (to add/remove targets)
    """
    global last_settings_mtime

    logger.info("Controller started. Watching targets and settings...")
    current_targets = []

    while True:
        try:
            # Check if mtr_script_settings.yaml changed (file mtime)
            current_mtime = os.path.getmtime(SCRIPT_SETTINGS_FILE)
            if current_mtime != last_settings_mtime:
                last_settings_mtime = current_mtime
                logger.info("Detected change in mtr_script_settings.yaml.")
                settings.update(load_settings(SCRIPT_SETTINGS_FILE))
                targets = load_targets()
                restart_all_monitors(targets)
                current_targets = targets
                time.sleep(1)
                continue

            # Load current targets
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

            current_targets = new_targets
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
