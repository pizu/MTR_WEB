#!/usr/bin/env python3
"""
controller.py — systemd-managed supervisor for MTR_WEB

- Watches mtr_targets.yaml + mtr_script_settings.yaml
- Manages one mtr_watchdog.py per active target (paused: true is respected)
- Runs the reporting pipeline (graphs → json → html → index)
  on a schedule and on configuration changes.
"""

import os
import sys
import time
import yaml
import signal
import subprocess
import threading
from datetime import datetime
from modules.utils import load_settings, setup_logger
from typing import Optional

# --- Paths -------------------------------------------------------------------
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
CONFIG_FILE = os.path.join(REPO_ROOT, "mtr_targets.yaml")
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")

MONITOR_SCRIPT         = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")
GRAPH_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT     = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")
HTML_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "index_generator.py")

PYTHON = sys.executable or "/usr/bin/python3"

# --- Init logging/settings ---------------------------------------------------
settings = load_settings(SETTINGS_FILE)
logger = setup_logger("controller", settings.get("log_directory", "/tmp"),
                      "controller.log", settings=settings)

def safe_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

last_targets_mtime  = safe_mtime(CONFIG_FILE)
last_settings_mtime = safe_mtime(SETTINGS_FILE)

# --- Targets loading ---------------------------------------------------------
def load_targets():
    """Return normalized list of targets from mtr_targets.yaml."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        out = []
        for t in (data.get("targets") or []):
            ip = str(t.get("ip", "")).strip()
            if not ip:
                continue
            out.append({
                "ip": ip,
                "description": t.get("description", ""),
                "source_ip": t.get("source_ip") or t.get("source"),
                "paused": bool(t.get("paused", False)),
            })
        return out
    except Exception as e:
        logger.error(f"Failed to read {CONFIG_FILE}: {e}")
        return []

# --- Worker management -------------------------------------------------------
class MonitorProc:
    def __init__(self, popen, ip, source_ip):
        self.popen = popen
        self.ip = ip
        self.source_ip = source_ip
        self.started = datetime.now()

monitored = {}  # ip -> MonitorProc
lock = threading.Lock()

def start_monitor(ip: str, source_ip: Optional[str]):
    """Start one mtr_watchdog.py for the IP, passing absolute settings path."""
    try:
        cmd = [PYTHON, MONITOR_SCRIPT, "--target", ip, "--settings", SETTINGS_FILE]
        if source_ip:
            cmd += ["--source", source_ip]
        logger.info(f"Starting monitor for {ip} (source={source_ip or '-'})")
        p = subprocess.Popen(cmd, cwd=SCRIPTS_DIR)
        return MonitorProc(p, ip, source_ip)
    except Exception as e:
        logger.error(f"Failed to start monitor for {ip}: {e}")
        return None

def stop_monitor(ip: str, reason="controller stop"):
    with lock:
        mon = monitored.pop(ip, None)
    if not mon:
        return
    try:
        if mon.popen.poll() is None:
            logger.info(f"{reason} — {ip} (pid={mon.popen.pid})")
            mon.popen.terminate()
            try:
                mon.popen.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning(f"{ip} did not exit; SIGKILL")
                mon.popen.kill()
    except Exception as e:
        logger.warning(f"Error stopping {ip}: {e}")

def stop_all():
    for ip in list(monitored.keys()):
        stop_monitor(ip, reason="shutdown")

def reconcile_monitors(targets: list[dict]):
    """Ensure running workers match active (non-paused) targets."""
    active = [t for t in targets if not t.get("paused")]
    desired = {t["ip"] for t in active}
    paused  = {t["ip"] for t in targets if t.get("paused")}

    with lock:
        current = set(monitored.keys())

    # start new
    for t in active:
        ip = t["ip"]
        if ip not in current:
            mp = start_monitor(ip, t.get("source_ip"))
            if mp:
                with lock:
                    monitored[ip] = mp

    # stop removed
    for ip in current - (desired | paused):
        stop_monitor(ip, reason="removed from config")

    # stop newly paused
    for ip in (current & paused):
        stop_monitor(ip, reason="paused in config")

# --- Pipeline ----------------------------------------------------------------
def run_step(name: str, script_path: str):
    """Run a pipeline step, passing absolute settings path as argv[1]."""
    try:
        logger.info(f"[pipeline] {name} …")
        p = subprocess.run([PYTHON, script_path, SETTINGS_FILE],
                           cwd=SCRIPTS_DIR, capture_output=True, text=True)
        if p.returncode != 0:
            logger.warning(f"{name} exited {p.returncode}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
        elif p.stdout.strip():
            logger.debug(f"{name} output: {p.stdout.strip()[:1000]}")
    except Exception as e:
        logger.warning(f"{name} failed: {e}")

def run_pipeline(reason="scheduled"):
    logger.info(f"Running reporting pipeline (reason: {reason})")
    run_step("graph_generator",     GRAPH_GENERATOR_SCRIPT)
    run_step("timeseries_exporter", TS_EXPORTER_SCRIPT)
    run_step("html_generator",      HTML_GENERATOR_SCRIPT)
    run_step("index_generator",     INDEX_GENERATOR_SCRIPT)

# --- Main loop ---------------------------------------------------------------
def main():
    global settings, last_targets_mtime, last_settings_mtime

    # Controller cadence (tunable in mtr_script_settings.yaml → controller.*)
    cfg = settings.get("controller", {}) or {}
    loop_seconds   = int(cfg.get("loop_seconds", 15))
    pipe_every_sec = int(cfg.get("pipeline_every_seconds", 120))
    rerun_on_changes = bool(cfg.get("rerun_pipeline_on_changes", True))

    logger.info("Controller starting …")
    logger.info(f"Repo: {REPO_ROOT}")
    logger.info(f"Loop={loop_seconds}s, pipeline={pipe_every_sec}s, rerun_on_changes={rerun_on_changes}")

    targets = load_targets()
    logger.info(f"Loaded {len(targets)} targets (initial)")
    reconcile_monitors(targets)

    # initial pipeline so UI has content after reboot
    last_pipeline = 0.0
    run_pipeline("startup")
    last_pipeline = time.time()

    stopping = {"flag": False}
    def handle_term(sig, frame):
        logger.info("Signal received; stopping …")
        stopping["flag"] = True
    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGINT, handle_term)

    while not stopping["flag"]:
        time.sleep(loop_seconds)

        # settings change
        cur_set_mtime = safe_mtime(SETTINGS_FILE)
        settings_changed = (cur_set_mtime != last_settings_mtime)
        if settings_changed:
            last_settings_mtime = cur_set_mtime
            settings = load_settings(SETTINGS_FILE)
            cfg = settings.get("controller", {}) or {}
            loop_seconds   = int(cfg.get("loop_seconds",   loop_seconds))
            pipe_every_sec = int(cfg.get("pipeline_every_seconds", pipe_every_sec))
            rerun_on_changes = bool(cfg.get("rerun_pipeline_on_changes", rerun_on_changes))
            logger.info(f"Settings changed. loop={loop_seconds}s, pipeline={pipe_every_sec}s, rerun_on_changes={rerun_on_changes}")
            # restart monitors so children pick up changes
            targets = load_targets()
            stop_all()
            reconcile_monitors(targets)

        # targets change
        cur_tgt_mtime = safe_mtime(CONFIG_FILE)
        targets_changed = (cur_tgt_mtime != last_targets_mtime)
        if targets_changed:
            last_targets_mtime = cur_tgt_mtime
            targets = load_targets()
            logger.info(f"Targets changed; now {len(targets)} total. Reconciling …")
            reconcile_monitors(targets)

        # reap/restart dead monitors
        with lock:
            for ip, mp in list(monitored.items()):
                if mp.popen.poll() is not None:
                    logger.warning(f"Monitor for {ip} exited with code {mp.popen.returncode}; restarting.")
                    stop_monitor(ip, reason="exited")
                    source_ip = next((t.get("source_ip") for t in targets if t["ip"] == ip), None)
                    new_mp = start_monitor(ip, source_ip)
                    if new_mp:
                        monitored[ip] = new_mp

        # scheduled / change-triggered pipeline
        now = time.time()
        due = (now - last_pipeline) >= pipe_every_sec
        if due or (rerun_on_changes and (targets_changed or settings_changed)):
            run_pipeline("scheduled" if due else "config-change")
            last_pipeline = time.time()

    # shutdown
    stop_all()
    logger.info("Controller stopped cleanly.")

if __name__ == "__main__":
    main()
