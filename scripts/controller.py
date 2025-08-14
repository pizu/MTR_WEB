#!/usr/bin/env python3
"""
controller.py — Supervisor for MTR_WEB

WHAT THIS DOES (high level):
  • Continuously watches two YAML files:
      - mtr_targets.yaml            (list of targets, with optional "paused: true")
      - mtr_script_settings.yaml    (controller/pipeline/paths/tunables)
  • Ensures there is ONE "mtr_watchdog.py" process per active (non-paused) target.
  • Periodically runs the "reporting pipeline":
      graph_generator.py → timeseries_exporter.py → html_generator.py → index_generator.py
  • Reacts quickly to config changes (targets or settings) and re-runs/refreshes as needed.
  • SHUTS DOWN CLEANLY: children are started in their own process-groups and are terminated
    as a group (TERM → short wait → KILL), so no lingering mtr/rrdtool children remain.

WHY IT WAS HANGING BEFORE:
  - Old version launched children in the controller’s process group. When you stopped the
    service while a long pipeline step was running, Python sat waiting. Also, a monitor
    might have spawned an 'mtr' child that outlived the parent, keeping the service alive.
  - This version fixes it by starting each child in its own session/process-group and
    explicitly signaling that whole group on shutdown or when aborting a running step.

PREREQUISITES:
  - Project layout:
      repo/
        mtr_targets.yaml
        mtr_script_settings.yaml
        scripts/
          controller.py   (this file)
          mtr_watchdog.py
          graph_generator.py
          timeseries_exporter.py
          html_generator.py
          index_generator.py
          modules/
            utils.py
            ...other modules...
  - Python 3.x environment with your usual deps.

SYSTEMD TIP:
  - In your service unit, prefer:
        KillMode=mixed
        TimeoutStopSec=20s
    The script already kills its children; these are just additional safety nets.
"""

# =========================
# Standard library imports
# =========================
import os
import sys
import time
import yaml
import signal
import subprocess
import threading
from datetime import datetime
from typing import Optional

# =========================
# Project utilities
# =========================
# We import load_settings() and setup_logger() from your shared utils module to:
#   - read the YAML once at startup (and again on change),
#   - create a rotating, formatted logger writing to the configured log directory.
from modules.utils import load_settings, setup_logger


# =========================
# Path constants
# =========================
# SCRIPTS_DIR: absolute path to the directory containing THIS controller file.
SCRIPTS_DIR   = os.path.abspath(os.path.dirname(__file__))
# REPO_ROOT:   parent directory that contains the YAML files and "scripts/".
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
# YAML paths (absolute). We always pass absolute paths to children.
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")

# Child script paths (absolute)
MONITOR_SCRIPT         = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")
GRAPH_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT     = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")
HTML_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "index_generator.py")

# Python interpreter we’ll use to spawn children
PYTHON = sys.executable or "/usr/bin/python3"


# =========================
# Logging & settings
# =========================
# We load settings immediately so we can initialize logging (log dir/name, level, etc.).
settings = load_settings(SETTINGS_FILE)
logger = setup_logger(
    "controller",
    settings.get("log_directory", "/tmp"),
    "controller.log",
    settings=settings
)

def _safe_mtime(path: str) -> float:
    """Return file modification time or 0.0 on error/missing (never raises)."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

last_targets_mtime  = _safe_mtime(CONFIG_FILE)
last_settings_mtime = _safe_mtime(SETTINGS_FILE)


# =========================
# Read targets YAML
# =========================
def load_targets() -> list[dict]:
    """
    Load and normalize targets from mtr_targets.yaml.

    Returns a list of dicts like:
      {
        "ip": "8.8.8.8",
        "description": "Google DNS",
        "source_ip": "192.0.2.10" | None,
        "paused": False
      }
    """
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        out: list[dict] = []
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


# =========================
# Process group helpers
# =========================
def _terminate_pgid(p: subprocess.Popen, name: str, grace: float = 8.0) -> None:
    """
    TERM → wait → KILL for a whole *process group*.

    WHY process groups?
      We create each child with start_new_session=True so it becomes the leader
      of its own session/process-group. Then we can kill the entire subtree
      (e.g., watchdog → mtr → rrdtool) in one shot.

    Args:
      p:     the Popen handle
      name:  for logs (e.g., "monitor:8.8.8.8" or "pipeline:graph_generator")
      grace: seconds to wait after TERM before KILL
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
                # Fallback: just terminate the pid
                logger.info(f"[stop] TERM pid {name} pid={p.pid}")
                p.terminate()

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


# =========================
# Monitor process tracking
# =========================
class MonitorProc:
    """
    Small container for a running monitor process.
    Helpful for logging & restarts (holds start time and source selection).
    """
    def __init__(self, popen: subprocess.Popen, ip: str, source_ip: Optional[str]):
        self.popen = popen
        self.ip = ip
        self.source_ip = source_ip
        self.started = datetime.now()

# Mapping: ip -> MonitorProc
monitored: dict[str, MonitorProc] = {}
# Lock protects the 'monitored' dict from concurrent access
lock = threading.Lock()


def start_monitor(ip: str, source_ip: Optional[str]) -> Optional[MonitorProc]:
    """
    Start one mtr_watchdog.py for the given IP.
    IMPORTANT: start_new_session=True creates a new process-group, letting us
               signal the whole group on shutdown.
    """
    try:
        cmd = [PYTHON, MONITOR_SCRIPT, "--target", ip, "--settings", SETTINGS_FILE]
        if source_ip:
            cmd += ["--source", source_ip]
        logger.info(f"Starting monitor for {ip} (source={source_ip or '-'})")
        p = subprocess.Popen(
            cmd,
            cwd=SCRIPTS_DIR,
            start_new_session=True,   # <<< critical for clean stop
            stdout=None, stderr=None, stdin=None,
            close_fds=True
        )
        return MonitorProc(p, ip, source_ip)
    except Exception as e:
        logger.error(f"Failed to start monitor for {ip}: {e}")
        return None


def stop_monitor(ip: str, reason: str = "controller stop") -> None:
    """
    Stop a specific monitor by IP (TERM → wait → KILL the process-group).
    Safe to call even if already stopped.
    """
    with lock:
        mon = monitored.pop(ip, None)
    if not mon:
        return
    p = mon.popen
    if p is None or p.poll() is not None:
        return
    logger.info(f"{reason} — {ip} (pid={p.pid})")
    _terminate_pgid(p, f"monitor:{ip}")


def stop_all() -> None:
    """Stop all running monitor children (best-effort, idempotent)."""
    with lock:
        ips = list(monitored.keys())
    for ip in ips:
        stop_monitor(ip, reason="shutdown")


def reconcile_monitors(targets: list[dict]) -> None:
    """
    Ensure the set of running monitors matches the *active* targets.
    Starts missing monitors, stops those removed or newly paused.

    Active = targets with paused: false (or missing).
    """
    active = [t for t in targets if not t.get("paused")]
    desired = {t["ip"] for t in active}
    paused  = {t["ip"] for t in targets if t.get("paused")}

    with lock:
        current = set(monitored.keys())

    # Start new ones
    for t in active:
        ip = t["ip"]
        if ip not in current:
            mp = start_monitor(ip, t.get("source_ip"))
            if mp:
                with lock:
                    monitored[ip] = mp

    # Stop those removed from config
    for ip in current - (desired | paused):
        stop_monitor(ip, reason="removed from config")

    # Stop those newly paused
    for ip in (current & paused):
        stop_monitor(ip, reason="paused in config")


# =========================
# Pipeline runner (abortable)
# =========================
_current_pipeline_proc: Optional[subprocess.Popen] = None
_current_pipeline_name: Optional[str] = None
_pipeline_lock = threading.Lock()

def _run_step_interruptible(name: str, script_path: str) -> int:
    """
    Run one pipeline step as its own process-group.
    We capture stdout/stderr for logging, but more importantly:
      - we can ABORT it instantly on shutdown via _terminate_pgid().
    Returns the process return code (0 on success).
    """
    global _current_pipeline_proc, _current_pipeline_name
    try:
        logger.info(f"[pipeline] {name} …")
        with _pipeline_lock:
            _current_pipeline_name = name
            _current_pipeline_proc = subprocess.Popen(
                [PYTHON, script_path, SETTINGS_FILE],
                cwd=SCRIPTS_DIR,
                start_new_session=True,  # <<< process-group for safe abort
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        p = _current_pipeline_proc
        stdout, stderr = p.communicate()  # blocks; we will kill on signal
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


def abort_running_pipeline() -> None:
    """
    If a pipeline step is currently running, kill its entire process-group.
    Called when we receive SIGTERM/SIGINT so shutdown is immediate.
    """
    with _pipeline_lock:
        p = _current_pipeline_proc
        name = _current_pipeline_name
    if p is None:
        return
    logger.info(f"[pipeline] aborting running step: {name}")
    _terminate_pgid(p, f"pipeline:{name}", grace=3.0)


def run_pipeline(reason: str = "scheduled") -> None:
    """
    Run all reporting steps in order. If one step fails, we still attempt the rest,
    but each step is isolated and abortable.
    """
    logger.info(f"Running reporting pipeline (reason: {reason})")
    _run_step_interruptible("graph_generator",     GRAPH_GENERATOR_SCRIPT)
    _run_step_interruptible("timeseries_exporter", TS_EXPORTER_SCRIPT)
    _run_step_interruptible("html_generator",      HTML_GENERATOR_SCRIPT)
    _run_step_interruptible("index_generator",     INDEX_GENERATOR_SCRIPT)


# =========================
# Main loop
# =========================
def main() -> None:
    global settings, last_targets_mtime, last_settings_mtime

    # Controller cadence from YAML (with safe defaults)
    cfg = settings.get("controller", {}) or {}
    loop_seconds       = int(cfg.get("loop_seconds", 15))                # how often the main loop ticks
    pipe_every_seconds = int(cfg.get("pipeline_every_seconds", 120))     # pipeline frequency
    rerun_on_changes   = bool(cfg.get("rerun_pipeline_on_changes", True))# also run pipeline on any config change

    logger.info("Controller starting …")
    logger.info(f"Repo: {REPO_ROOT}")
    logger.info(f"Loop={loop_seconds}s, pipeline={pipe_every_seconds}s, rerun_on_changes={rerun_on_changes}")

    # Initial reconcile + pipeline so the UI has content after reboot
    targets = load_targets()
    logger.info(f"Loaded {len(targets)} targets (initial)")
    reconcile_monitors(targets)
    last_pipeline = 0.0
    run_pipeline("startup")
    last_pipeline = time.time()

    # Stop flag toggled by signal handlers
    stopping = {"flag": False}

    def handle_term(sig, frame):
        """
        On SIGTERM/SIGINT:
          • Set stop flag
          • Abort any running pipeline step *immediately*
          • The loop will then stop monitors as soon as it wakes up
        """
        logger.info(f"Signal {sig} received; stopping …")
        stopping["flag"] = True
        abort_running_pipeline()

    # Register signal handlers (Ctrl-C, systemd stop)
    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGINT,  handle_term)

    # Main controller loop
    while not stopping["flag"]:
        # Sleep in small slices so stop is responsive even during sleep
        remaining = float(loop_seconds)
        while remaining > 0 and not stopping["flag"]:
            time.sleep(min(0.5, remaining))
            remaining -= 0.5
        if stopping["flag"]:
            break

        # Detect settings changes
        cur_set_mtime = _safe_mtime(SETTINGS_FILE)
        settings_changed = (cur_set_mtime != last_settings_mtime)
        if settings_changed:
            last_settings_mtime = cur_set_mtime
            settings = load_settings(SETTINGS_FILE)
            cfg = settings.get("controller", {}) or {}
            loop_seconds       = int(cfg.get("loop_seconds",       loop_seconds))
            pipe_every_seconds = int(cfg.get("pipeline_every_seconds", pipe_every_seconds))
            rerun_on_changes   = bool(cfg.get("rerun_pipeline_on_changes", rerun_on_changes))
            logger.info(f"Settings changed. loop={loop_seconds}s, pipeline={pipe_every_seconds}s, rerun_on_changes={rerun_on_changes}")
            # Restart monitors so new settings apply to children
            targets = load_targets()
            stop_all()
            reconcile_monitors(targets)

        # Detect target list changes
        cur_tgt_mtime = _safe_mtime(CONFIG_FILE)
        targets_changed = (cur_tgt_mtime != last_targets_mtime)
        if targets_changed:
            last_targets_mtime = cur_tgt_mtime
            targets = load_targets()
            logger.info(f"Targets changed; now {len(targets)} total. Reconciling …")
            reconcile_monitors(targets)

        # Reap/restart any monitor that has died (best-effort resiliency)
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

        # Scheduled or change-triggered pipeline
        now = time.time()
        due = (now - last_pipeline) >= pipe_every_seconds
        if not stopping["flag"] and (due or (rerun_on_changes and (targets_changed or settings_changed))):
            run_pipeline("scheduled" if due else "config-change")
            last_pipeline = time.time()

    # ===== Shutdown path =====
    logger.info("Stopping monitors …")
    stop_all()
    abort_running_pipeline()
    logger.info("Controller stopped cleanly.")


# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    main()
