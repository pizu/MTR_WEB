#!/usr/bin/env python3
#
# controller.py
#
# Watches mtr_targets.yaml and mtr_script_settings.yaml.
# Starts/stops mtr_watchdog.py processes based on target or settings changes.
# Supports per-target "paused: true|false" (default false).

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # Allow local imports from scripts/modules
import yaml
import time
import subprocess
import threading
from modules.utils import load_settings, setup_logger

# Constants
CONFIG_FILE = "mtr_targets.yaml"
SCRIPT_SETTINGS_FILE = "mtr_script_settings.yaml"
MONITOR_SCRIPT = "scripts/mtr_watchdog.py"  # entrypoint

# Globals
settings = load_settings(SCRIPT_SETTINGS_FILE)
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("controller", log_directory, "controller.log", settings=settings)

monitored_targets = {}  # ip -> subprocess.Popen
lock = threading.Lock()
last_settings_mtime = os.path.getmtime(SCRIPT_SETTINGS_FILE)

def load_targets():
    """
    Reads the YAML file with the list of targets.
    Returns a list of target dicts with:
      - ip (str)
      - description (optional)
      - source_ip (optional)
      - paused (bool, default False)
    """
    try:
        with open(CONFIG_FILE, "r") as f:
            data = yaml.safe_load(f) or {}
            targets = data.get("targets", []) or []
            # Normalize + defaults
            out = []
            for t in targets:
                ip = str(t.get("ip", "")).strip()
                if not ip:
                    continue
                out.append({
                    "ip": ip,
                    "description": t.get("description", ""),
                    "source_ip": t.get("source_ip"),
                    "paused": bool(t.get("paused", False)),
                })
            return out
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

def stop_monitor(ip, reason="Stopped"):
    """
    Stops the subprocess for a specific target IP.
    """
    with lock:
        proc = monitored_targets.pop(ip, None)
        if proc:
            try:
                logger.info(f"{reason} monitor for {ip} (PID: {proc.pid})")
                proc.terminate()
            except Exception as e:
                logger.error(f"Failed to stop monitor for {ip}: {e}")

def stop_all():
    """
    Stops all running monitor subprocesses.
    """
    with lock:
        for ip, proc in list(monitored_targets.items()):
            try:
                logger.info(f"Terminating monitor for {ip} (PID: {proc.pid})")
                proc.terminate()
            except Exception as e:
                logger.error(f"Error terminating process for {ip}: {e}")
            finally:
                monitored_targets.pop(ip, None)

def restart_all_monitors(targets):
    """
    Restarts all monitor subprocesses — used when settings.yaml is updated.
    Skips paused targets.
    """
    logger.info("Settings changed. Restarting all monitors.")
    stop_all()
    time.sleep(1)
    for t in targets:
        if t.get("paused", False):
            logger.info(f"[{t['ip']}] Skipping (paused).")
            continue
        proc = start_monitor(t)
        if proc:
            monitored_targets[t["ip"]] = proc

def monitor_loop():
    """
    Main controller loop that checks for changes to:
    - mtr_script_settings.yaml (to trigger full restarts)
    - mtr_targets.yaml (to add/remove/stop/start targets; honors paused)
    """
    global last_settings_mtime

    logger.info("Controller started. Watching targets and settings...")
    while True:
        try:
            # React to settings changes (mtime check)
            current_mtime = os.path.getmtime(SCRIPT_SETTINGS_FILE)
            if current_mtime != last_settings_mtime:
                last_settings_mtime = current_mtime
                logger.info("Detected change in mtr_script_settings.yaml.")
                settings.update(load_settings(SCRIPT_SETTINGS_FILE))
                targets = load_targets()
                restart_all_monitors(targets)
                time.sleep(1)
                continue

            # Load/normalize current targets
            targets = load_targets()
            active_targets = [t for t in targets if not t.get("paused", False)]
            paused_ips = {t["ip"] for t in targets if t.get("paused", False)}
            new_ips = {t["ip"] for t in active_targets}

            with lock:
                cur_ips = set(monitored_targets.keys())

            # Start monitors for newly active IPs
            for t in active_targets:
                ip = t["ip"]
                if ip not in cur_ips:
                    proc = start_monitor(t)
                    if proc:
                        with lock:
                            monitored_targets[ip] = proc

            # Stop monitors for removed IPs
            for ip in cur_ips - (new_ips | paused_ips):
                stop_monitor(ip, reason="Removed from config:")

            # Stop monitors that are now paused
            for ip in (cur_ips & paused_ips):
                stop_monitor(ip, reason="Paused in config:")

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
