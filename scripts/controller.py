#!/usr/bin/env python3
"""
controller.py — systemd-managed supervisor for MTR_WEB

- Watches mtr_targets.yaml + mtr_script_settings.yaml
- Manages one mtr_watchdog.py per active target (paused: true is respected)
- Runs the reporting pipeline (graphs → json → html → index)
  on a schedule and on configuration changes.

This version improves shutdown behavior:
- All children (monitors + pipeline steps) run in their own process groups.
- On SIGTERM/SIGINT we signal the whole process groups (TERM → KILL if needed).
- If a pipeline step is running during shutdown, we abort it immediately.
"""

import os
import sys
import time
import yaml
import signal
import subprocess
import threading
from datetime import datetime
from typing import Optional

# Import project utils (logging + settings loader)
from modules.utils import load_settings, setup_logger

# --- Paths -------------------------------------------------------------------
SCRIPTS_DIR   = os.path.abspath(os.path.dirname(__file__))                 # scripts/
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))      # repo root
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")
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
    """Return mtime or 0.0 if missing/error (avoids exceptions in the loop)."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

last_targets_mtime  = safe_mtime(CONFIG_FILE)
last_settings_mtime = safe_mtime(SETTINGS_FILE)

# --- Targets loading ---------------------------------------------------------
def load_targets():
    """
    Return normalized list of targets from mtr_targets.yaml.
    Each item: {ip, description, source_ip, paused}
    """
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

# --- Process helpers (important for clean shutdown) --------------------------
def _terminate_pgid(p: subprocess.Popen, name: str, grace: float = 8.0):
    """
    Terminate a whole process group for a child. We start children with
    start_new_session=True so they become PGID leaders. We first send SIGTERM,
    wait up to `grace` seconds, then SIGKILL if still alive.
    """
    if p is None:
        return
    try:
        if p.poll() is None:
            try:
                pgid = os.getpgid(p.pid)
            except Exception:
                pgid = None
            if pgid is not None:
                logger.info(f"[stop] TERM process-group {name} pgid={pgid}")
                os.killpg(pgid, signal.SIGTERM)
            else:
                logger.info(f"[stop] TERM pid {name} pid={p.pid}")
                p.terminate()

            # Wait for graceful exit
            try:
                p.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                logger.warning(f"[stop] {name} still running after {grace:.0f}s; KILLing")
                try:
                    if pgid is not None:
                        os.killpg(pgid, signal.SIGKILL)
                    else:
                        p.kill()
                except Exception as ke:
                    logger.warning(f"[stop] kill failed for {name}: {ke}")
    except Exception as e:
        logger.warning(f"[stop] error terminating {name}: {e}")

# --- Worker management -------------------------------------------------------
class MonitorProc:
    """Simple holder for a running monitor child."""
    def __init__(self, popen: subprocess.Popen, ip: str, source_ip: Optional[str]):
        self.popen = popen
        self.ip = ip
        self.source_ip = source_ip
        self.started = datetime.now()

monitored: dict[str, MonitorProc] = {}
lock = threading.Lock()

def start_monitor(ip: str, source_ip: Optional[str]) -> Optional[MonitorProc]:
    """
    Start one mtr_watchdog.py for the IP, passing absolute settings path.
    We start it in a new session (own process group) for clean signaling.
    """
    try:
        cmd = [PYTHON, MONITOR_SCRIPT, "--target", ip, "--settings", SETTINGS_FILE]
        if source_ip:
            cmd += ["--source", source_ip]
        logger.info(f"Starting monitor for {ip} (source={source_ip or '-'})")
        p = subprocess.Popen(
            cmd,
            cwd=SCRIPTS_DIR,
            start_new_session=True,   # <-- critical for group signaling
            stdout=None, stderr=None, stdin=None,
            close_fds=True
        )
        return MonitorProc(p, ip, source_ip)
    except Exception as e:
        logger.error(f"Failed to start monitor for {ip}: {e}")
        return None

def stop_monitor(ip: str, reason="controller stop"):
    """Stop a specific monitor (TERM group → KILL)."""
    with lock:
        mon = monitored.pop(ip, None)
    if not mon:
        return
    p = mon.popen
    if p is None:
        return
    if p.poll() is not None:
        return
    logger.info(f"{reason} — {ip} (pid={p.pid})")
    _terminate_pgid(p, f"monitor:{ip}")

def stop_all():
    """Stop all running monitors."""
    with lock:
        ips = list(monitored.keys())
    for ip in ips:
        stop_monitor(ip, reason="shutdown")

def reconcile_monitors(targets: list[dict]):
    """
    Ensure running workers match active (non-paused) targets.
    Start missing, stop removed/paused.
    """
    active = [t for t in targets if not t.get("paused")]
    desired = {t["ip"] for t in active}
    paused  = {t["ip"] for t in targets if t.get("paused")}

    with lock:
        current = set(monitored.keys())

    # Start new
    for t in active:
        ip = t["ip"]
        if ip not in current:
            mp = start_monitor(ip, t.get("source_ip"))
            if mp:
                with lock:
                    monitored[ip] = mp

    # Stop removed
    for ip in current - (desired | paused):
        stop_monitor(ip, reason="removed from config")

    # Stop newly paused
    for ip in (current & paused):
        stop_monitor(ip, reason="paused in config")

# --- Pipeline (run steps with interruptibility) ------------------------------
_current_pipeline_proc: Optional[subprocess.Popen] = None
_current_pipeline_name: Optional[str] = None
_pipeline_lock = threading.Lock()

def _run_step_interruptible(name: str, script_path: str) -> int:
    """
    Run a pipeline step with start_new_session=True so we can nuke its whole
    process group if we need to stop quickly. Returns the exit code.
    """
    global _current_pipeline_proc, _current_pipeline_name
    try:
        logger.info(f"[pipeline] {name} …")
        with _pipeline_lock:
            _current_pipeline_name = name
            _current_pipeline_proc = subprocess.Popen(
                [PYTHON, script_path, SETTINGS_FILE],
                cwd=SCRIPTS_DIR,
                start_new_session=True,   # <-- key for clean stop
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        p = _current_pipeline_proc
        stdout, stderr = p.communicate()  # blocks, but we can kill the group on stop
        rc = p.returncode
        if rc != 0:
            logger.warning(f"{name} exited {rc}\nstdout:\n{stdout}\nstderr:\n{stderr}")
        elif stdout.strip():
            logger.debug(f"{name} output: {stdout.strip()[:1000]}")
        return rc
    except Exception as e:
        logger.warning(f"{name} failed: {e}")
        return 1
    finally:
        with _pipeline_lock:
            _current_pipeline_proc = None
            _current_pipeline_name = None

def abort_running_pipeline():
    """
    If a pipeline step is currently running, kill its group.
    This is called during shutdown to avoid waiting for long steps.
    """
    with _pipeline_lock:
        p = _current_pipeline_proc
        name = _current_pipeline_name
    if p is None:
        return
    logger.info(f"[pipeline] aborting running step: {name}")
    _terminate_pgid(p, f"pipeline:{name}", grace=3.0)

def run_pipeline(reason="scheduled"):
    """Run the full reporting pipeline, step-by-step."""
    logger.info(f"Running reporting pipeline (reason: {reason})")
    # If shutdown was requested between steps, the main loop will bail out.
    _run_step_interruptible("graph_generator",     GRAPH_GENERATOR_SCRIPT)
    _run_step_interruptible("timeseries_exporter", TS_EXPORTER_SCRIPT)
    _run_step_interruptible("html_generator",      HTML_GENERATOR_SCRIPT)
    _run_step_interruptible("index_generator",     INDEX_GENERATOR_SCRIPT)

# --- Main loop ---------------------------------------------------------------
def main():
    global settings, last_targets_mtime, last_settings_mtime

    # Controller cadence (tunable in mtr_script_settings.yaml → controller.*)
    cfg = settings.get("controller", {}) or {}
    loop_seconds       = int(cfg.get("loop_seconds", 15))
    pipe_every_seconds = int(cfg.get("pipeline_every_seconds", 120))
    rerun_on_changes   = bool(cfg.get("rerun_pipeline_on_changes", True))

    logger.info("Controller starting …")
    logger.info(f"Repo: {REPO_ROOT}")
    logger.info(f"Loop={loop_seconds}s, pipeline={pipe_every_seconds}s, rerun_on_changes={rerun_on_changes}")

    targets = load_targets()
    logger.info(f"Loaded {len(targets)} targets (initial)")
    reconcile_monitors(targets)

    # Initial pipeline so UI has content after reboot
    last_pipeline = 0.0
    run_pipeline("startup")
    last_pipeline = time.time()

    # Stop flag toggled by signal handlers
    stopping = {"flag": False}

    def handle_term(sig, frame):
        """
        On SIGTERM/SIGINT:
        - Flip stop flag
        - Abort any running pipeline step immediately
        - (The loop will proceed to stop monitors cleanly)
        """
        logger.info(f"Signal {sig} received; stopping …")
        stopping["flag"] = True
        abort_running_pipeline()
    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGINT, handle_term)

    while not stopping["flag"]:
        # Sleep in short slices so we can react quickly to stop flag
        # (signals typically interrupt sleep, but this makes it snappier).
        remaining = float(loop_seconds)
        while remaining > 0 and not stopping["flag"]:
            time.sleep(min(0.5, remaining))
            remaining -= 0.5
        if stopping["flag"]:
            break

        # Settings change?
        cur_set_mtime = safe_mtime(SETTINGS_FILE)
        settings_changed = (cur_set_mtime != last_settings_mtime)
        if settings_changed:
            last_settings_mtime = cur_set_mtime
            settings = load_settings(SETTINGS_FILE)
            cfg = settings.get("controller", {}) or {}
            loop_seconds       = int(cfg.get("loop_seconds",       loop_seconds))
            pipe_every_seconds = int(cfg.get("pipeline_every_seconds", pipe_every_seconds))
            rerun_on_changes   = bool(cfg.get("rerun_pipeline_on_changes", rerun_on_changes))
            logger.info(f"Settings changed. loop={loop_seconds}s, pipeline={pipe_every_seconds}s, rerun_on_changes={rerun_on_changes}")
            # Restart monitors so children pick up changes
            targets = load_targets()
            stop_all()
            reconcile_monitors(targets)

        # Targets change?
        cur_tgt_mtime = safe_mtime(CONFIG_FILE)
        targets_changed = (cur_tgt_mtime != last_targets_mtime)
        if targets_changed:
            last_targets_mtime = cur_tgt_mtime
            targets = load_targets()
            logger.info(f"Targets changed; now {len(targets)} total. Reconciling …")
            reconcile_monitors(targets)

        # Reap/restart dead monitors
        with lock:
            items = list(monitored.items())
        for ip, mp in items:
            if mp.popen.poll() is not None:
                logger.warning(f"Monitor for {ip} exited with code {mp.popen.returncode}; restarting.")
                stop_monitor(ip, reason="exited")
                source_ip = next((t.get("source_ip") for t in targets if t["ip"] == ip), None)
                new_mp = start_monitor(ip, source_ip)
                if new_mp:
                    with lock:
                        monitored[ip] = new_mp

        # Scheduled / change-triggered pipeline
        now = time.time()
        due = (now - last_pipeline) >= pipe_every_seconds
        if not stopping["flag"] and (due or (rerun_on_changes and (targets_changed or settings_changed))):
            run_pipeline("scheduled" if due else "config-change")
            last_pipeline = time.time()

    # Shutdown path
    logger.info("Stopping monitors …")
    stop_all()
    abort_running_pipeline()
    logger.info("Controller stopped cleanly.")

if __name__ == "__main__":
    main()
