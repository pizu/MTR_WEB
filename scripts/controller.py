#!/usr/bin/env python3
#
# controller.py  — systemd-friendly supervisor
#
# What it does:
#   1) Watches mtr_targets.yaml and mtr_script_settings.yaml for changes.
#   2) Starts/stops one mtr_watchdog.py worker per active target (honors paused: true).
#   3) Runs the reporting pipeline on a cadence and also when config changes:
#        - graph_generator.py
#        - timeseries_exporter.py      (exports html/data/<ip>_<range>.json for the interactive UI)
#        - html_generator.py
#        - index_generator.py
#
# Why it’s safe:
#   - Uses absolute paths (works regardless of WorkingDirectory).
#   - Best-effort pipeline: logs issues and keeps going.
#   - Detailed logging via modules.utils.setup_logger().
#
# Optional YAML knobs inside mtr_script_settings.yaml:
#   controller:
#     loop_seconds: 15
#     pipeline_every_seconds: 120
#     rerun_pipeline_on_changes: true
#
# If 'controller' is missing, sensible defaults are used.

import os
import sys
import time
import yaml
import signal
import subprocess
import threading
from datetime import datetime
from modules.utils import load_settings, setup_logger

# ------------------------------- Path resolution ------------------------------

# Absolute path to .../scripts/ (this file lives here)
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
# Repo root is one level up from scripts/
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))

# Absolute important files (robust whether systemd runs from repo root or anywhere)
CONFIG_FILE          = os.path.join(REPO_ROOT, "mtr_targets.yaml")
SCRIPT_SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")

# Child scripts (absolute)
MONITOR_SCRIPT          = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")
GRAPH_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT      = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")  # <— NEW
HTML_GENERATOR_SCRIPT   = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "index_generator.py")

# Python interpreter (what we’re currently running under)
PYTHON = sys.executable or "/usr/bin/python3"

# ------------------------------- Globals -------------------------------------

settings = load_settings(SCRIPT_SETTINGS_FILE)
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("controller", log_directory, "controller.log", settings=settings)

monitored_targets = {}  # { ip: Popen }
lock = threading.Lock()

def safe_mtime(path: str) -> float:
    """Return file mtime or 0.0 if the file does not exist."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

last_settings_mtime = safe_mtime(SCRIPT_SETTINGS_FILE)
last_targets_mtime  = safe_mtime(CONFIG_FILE)

# ------------------------------- YAML loading --------------------------------

def load_targets():
    """
    Read mtr_targets.yaml and normalize to:
      { "ip": str, "description": str, "source_ip": str|None, "paused": bool }
    """
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        items = []
        for t in (data.get("targets", []) or []):
            ip = str(t.get("ip", "")).strip()
            if not ip:
                continue
            items.append({
                "ip": ip,
                "description": t.get("description", ""),
                "source_ip": t.get("source_ip"),
                "paused": bool(t.get("paused", False)),
            })
        return items
    except Exception as e:
        logger.error(f"Failed to load targets from {CONFIG_FILE}: {e}")
        return []

# ------------------------------- Monitor mgmt --------------------------------

class MonitorProc:
    """Track a running mtr_watchdog worker."""
    def __init__(self, popen, ip, source_ip):
        self.popen = popen
        self.ip = ip
        self.source_ip = source_ip
        self.started = datetime.now()

def start_monitor(target):
    """
    Start a worker process for one target.
    We pass --settings to ensure the child reads the same YAML.
    """
    ip = target["ip"]
    source_ip = target.get("source_ip")
    try:
        cmd = [PYTHON, MONITOR_SCRIPT, "--target", ip, "--settings", SCRIPT_SETTINGS_FILE]
        if source_ip:
            cmd += ["--source", source_ip]
        logger.info(f"Starting monitor for {ip} (source={source_ip or '-'})")
        popen = subprocess.Popen(cmd, cwd=SCRIPTS_DIR)
        return MonitorProc(popen, ip, source_ip)
    except Exception as e:
        logger.error(f"Failed to start monitor for {ip}: {e}")
        return None

def stop_monitor(ip, reason="Stopped by controller"):
    """Stop a specific worker, if running."""
    with lock:
        mon = monitored_targets.pop(ip, None)
    if mon and mon.popen:
        try:
            logger.info(f"{reason} — {ip} (pid={mon.popen.pid})")
            mon.popen.terminate()
            try:
                mon.popen.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning(f"{ip} did not exit in time; sending SIGKILL")
                mon.popen.kill()
        except Exception as e:
            logger.warning(f"Error stopping {ip}: {e}")

def stop_all():
    """Stop all workers."""
    with lock:
        ips = list(monitored_targets.keys())
    for ip in ips:
        stop_monitor(ip, reason="Controller shutdown")

def reconcile_monitors(targets):
    """
    Ensure the set of running workers matches the active target list:
      - Start for new active targets.
      - Stop for removed/paused targets.
    """
    active = [t for t in targets if not t.get("paused", False)]
    desired_ips = {t["ip"] for t in active}
    paused_ips  = {t["ip"] for t in targets if t.get("paused", False)}

    with lock:
        current_ips = set(monitored_targets.keys())

    # Start new ones
    for t in active:
        ip = t["ip"]
        if ip not in current_ips:
            mon = start_monitor(t)
            if mon:
                with lock:
                    monitored_targets[ip] = mon

    # Stop removed
    for ip in current_ips - (desired_ips | paused_ips):
        stop_monitor(ip, reason="Removed from config")

    # Stop newly paused
    for ip in (current_ips & paused_ips):
        stop_monitor(ip, reason="Paused in config")

# ------------------------------ Pipeline runner ------------------------------

def run_step(name: str, script_path: str):
    """Run a single pipeline step; log stdout/stderr, don’t raise."""
    try:
        logger.info(f"[pipeline] running {name} …")
        p = subprocess.run([PYTHON, script_path, SCRIPT_SETTINGS_FILE],
                           cwd=SCRIPTS_DIR, capture_output=True, text=True)
        if p.returncode != 0:
            logger.warning(f"{name} exited {p.returncode}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
        else:
            # Keep logs tidy; each script also logs to its own file.
            if p.stdout.strip():
                logger.debug(f"{name} output: {p.stdout.strip()[:1000]}")
    except Exception as e:
        logger.warning(f"{name} failed: {e}")

def run_pipeline(reason="scheduled"):
    """
    Full reporting pipeline in order:
      graph_generator  →  timeseries_exporter  →  html_generator  →  index_generator
    """
    logger.info(f"Running reporting pipeline (reason: {reason})")
    run_step("graph_generator",     GRAPH_GENERATOR_SCRIPT)
    run_step("timeseries_exporter", TS_EXPORTER_SCRIPT)   # <— JSON for interactive graphs
    run_step("html_generator",      HTML_GENERATOR_SCRIPT)
    run_step("index_generator",     INDEX_GENERATOR_SCRIPT)

# ------------------------------- Main loop -----------------------------------

def monitor_loop():
    global settings, last_settings_mtime, last_targets_mtime

    logger.info("Controller started.")
    logger.info(f"Repo root   : {REPO_ROOT}")
    logger.info(f"Scripts dir : {SCRIPTS_DIR}")

    # Controller cadence knobs (with defaults if missing)
    ctrl_cfg = settings.get("controller", {}) or {}
    loop_seconds   = int(ctrl_cfg.get("loop_seconds", 15))
    pipe_every_sec = int(ctrl_cfg.get("pipeline_every_seconds", 120))
    rerun_on_changes = bool(ctrl_cfg.get("rerun_pipeline_on_changes", True))

    logger.info(f"Loop every {loop_seconds}s; pipeline every {pipe_every_sec}s; rerun_on_changes={rerun_on_changes}")

    # Initial state
    targets = load_targets()
    logger.info(f"Loaded {len(targets)} targets (initial)")
    reconcile_monitors(targets)

    # Kick pipeline once at startup so UI has content after reboot
    last_pipeline = 0.0
    run_pipeline(reason="startup")
    last_pipeline = time.time()

    # Graceful shutdown flags
    stopping = {"flag": False}

    def handle_term(signum, frame):
        logger.info("Received termination signal — shutting down …")
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGINT, handle_term)

    while not stopping["flag"]:
        time.sleep(loop_seconds)

        # Detect settings change
        cur_settings_mtime = safe_mtime(SCRIPT_SETTINGS_FILE)
        settings_changed = (cur_settings_mtime != last_settings_mtime)
        if settings_changed:
            last_settings_mtime = cur_settings_mtime
            logger.info("Detected change in mtr_script_settings.yaml; reloading.")
            settings = load_settings(SCRIPT_SETTINGS_FILE)

            # Refresh controller knobs immediately
            ctrl_cfg = settings.get("controller", {}) or {}
            loop_seconds   = int(ctrl_cfg.get("loop_seconds",   loop_seconds))
            pipe_every_sec = int(ctrl_cfg.get("pipeline_every_seconds", pipe_every_sec))
            rerun_on_changes = bool(ctrl_cfg.get("rerun_pipeline_on_changes", rerun_on_changes))
            logger.info(f"New controller cfg — loop={loop_seconds}s, pipeline_every={pipe_every_sec}s, rerun_on_changes={rerun_on_changes}")

            # Restart monitors to pick up new settings everywhere
            targets = load_targets()
            logger.info("Restarting all monitors due to settings change.")
            # Stop all then start active ones
            stop_all()
            reconcile_monitors(targets)

        # Detect targets change
        cur_targets_mtime = safe_mtime(CONFIG_FILE)
        targets_changed = (cur_targets_mtime != last_targets_mtime)
        if targets_changed:
            last_targets_mtime = cur_targets_mtime
            targets = load_targets()
            logger.info(f"Targets changed; now {len(targets)} total. Reconciling …")
            reconcile_monitors(targets)

        # Reap and auto-restart any dead workers
        with lock:
            for ip, mon in list(monitored_targets.items()):
                if mon.popen.poll() is not None:
                    logger.warning(f"Monitor for {ip} exited with code {mon.popen.returncode}; restarting.")
                    stop_monitor(ip, reason="Exited")
                    # Find the current source_ip for this IP from targets (if any)
                    source_ip = next((t.get("source_ip") for t in targets if t["ip"] == ip), None)
                    new_mon = start_monitor({"ip": ip, "source_ip": source_ip})
                    if new_mon:
                        monitored_targets[ip] = new_mon

        # Decide if pipeline is due
        now = time.time()
        due = (now - last_pipeline) >= pipe_every_sec
        if due or (rerun_on_changes and (targets_changed or settings_changed)):
            run_pipeline(reason=("scheduled" if due else "config-change"))
            last_pipeline = time.time()

    # Shutdown path
    logger.info("Stopping all monitors …")
    stop_all()
    logger.info("Controller stopped cleanly.")

# ------------------------------- Entrypoint ----------------------------------

if __name__ == "__main__":
    monitor_loop()
