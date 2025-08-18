#!/usr/bin/env python3
"""
controller.py
-------------
Central supervisor that:
- Reads mtr_targets.yaml (list of targets) and mtr_script_settings.yaml (global config).
- Starts one child process (mtr_watchdog.py) per *active* target.
- Stops child processes for removed or paused targets.
- Hot-reloads settings and targets when files change.
- Refreshes the 'controller' logger level dynamically (no restart needed).

USAGE:
    python3 scripts/controller.py

SERVICE:
    Use a systemd unit that sets WorkingDirectory to the repo root and ExecStart to
    /usr/bin/python3 scripts/controller.py

REQUIREMENTS:
    - Python 3.7+
    - modules/utils.py providing load_settings(), setup_logger(), refresh_logger_levels()
    - scripts/mtr_watchdog.py present and executable
"""

import os
import sys
import time
import yaml
import signal
import subprocess
import threading
from typing import Optional, List, Dict
from datetime import datetime

# --- Import our shared helpers (logging + settings) ---
# Ensure "scripts/modules" is importable when run via systemd or from elsewhere
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

from modules.utils import load_settings, setup_logger, refresh_logger_levels  # noqa: E402

# ----------------------------
# Paths and constants (all absolute)
# ----------------------------
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))   # repo root (one level up from scripts/)
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")             # targets file at repo root
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")     # settings file at repo root

MONITOR_SCRIPT         = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")   # child entrypoint per target
GRAPH_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT     = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")
HTML_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "index_generator.py")

PYTHON = sys.executable or "/usr/bin/python3"  # interpreter path

# ----------------------------
# Load initial settings & create logger (ORDER MATTERS!)
# ----------------------------
settings = load_settings(SETTINGS_FILE)

# Initialize 'controller' logger. Its level is taken from:
#   mtr_script_settings.yaml -> logging_levels.controller
logger = setup_logger(
    "controller",
    settings.get("log_directory", "/tmp"),
    "controller.log",
    settings=settings
)

# OPTIONAL: Pre-create the shared 'rrd' logger so rrd_handler fallback obeys YAML.
# If you use logging.getLogger("rrd") anywhere (e.g., rrd_handler.py), uncomment this.
# setup_logger("rrd", settings.get("log_directory", "/tmp"), "rrd.log", settings=settings)

# ----------------------------
# State tracking
# ----------------------------
# Map "ip" -> process info (child Popen + metadata), e.g.:
#   monitored_targets["8.8.8.8"] = {"proc": Popen, "source_ip": "192.0.2.10", "paused": False}
monitored_targets: Dict[str, Dict] = {}
lock = threading.Lock()

# Track last known modification times to detect changes
def _safe_mtime(path: str) -> float:
    """Return mtime if available, else 0.0 (so we can compare safely)."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

last_targets_mtime  = _safe_mtime(CONFIG_FILE)
last_settings_mtime = _safe_mtime(SETTINGS_FILE)

# ----------------------------
# Helpers: reading files
# ----------------------------
def load_targets() -> List[Dict]:
    """
    Load and normalize targets from mtr_targets.yaml.
    Returns a list of dicts:
      {
        "ip": "8.8.8.8",
        "description": "Google DNS",
        "source_ip": "192.0.2.10" or None,
        "paused": False
      }
    """
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        out: List[Dict] = []
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

# ----------------------------
# Child management
# ----------------------------
def start_watchdog(ip: str, source_ip: Optional[str]) -> Optional[subprocess.Popen]:
    """
    Start one mtr_watchdog instance for the given target.
    Args:
        ip: target IP or hostname
        source_ip: optional source IP to bind MTR
    Returns:
        subprocess.Popen of the child on success, or None on failure.
    """
    try:
        # Build argument list for the child
        args = [PYTHON, MONITOR_SCRIPT, "--target", ip, "--settings", SETTINGS_FILE]
        if source_ip:
            args.extend(["--source", str(source_ip)])

        # Start the child process detached from stdin; inherit environment
        proc = subprocess.Popen(
            args,
            cwd=REPO_ROOT,            # ensure relative paths inside child resolve to repo root
            stdout=subprocess.DEVNULL,  # rely on file logging; keep services quiet
            stderr=subprocess.DEVNULL
        )
        logger.info(f"Started watchdog for {ip} (PID {proc.pid}) args={args}")
        return proc
    except Exception as e:
        logger.error(f"Failed to start watchdog for {ip}: {e}")
        return None

def stop_watchdog(ip: str) -> None:
    """
    Stop and remove the watchdog process for a target (if running).
    Sends SIGTERM and waits briefly; escalates to kill if needed.
    """
    with lock:
        info = monitored_targets.get(ip)
        if not info:
            return
        proc: subprocess.Popen = info.get("proc")
        if proc and proc.poll() is None:
            try:
                logger.info(f"Stopping watchdog for {ip} (PID {proc.pid})")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Watchdog for {ip} did not exit in time; killing.")
                    proc.kill()
            except Exception as e:
                logger.error(f"Error while stopping watchdog for {ip}: {e}")
        monitored_targets.pop(ip, None)

def reconcile_targets(current: List[Dict]) -> None:
    """
    Compare desired targets (from YAML) with current child processes, then:
      - start missing processes for active targets,
      - stop processes for removed/paused targets,
      - restart process if source_ip changed.
    """
    # Build a dictionary keyed by IP for quick diffing
    desired = {t["ip"]: t for t in current}

    # 1) Stop watchdogs for targets that no longer exist or are paused
    for ip in list(monitored_targets.keys()):
        want = desired.get(ip)
        if (want is None) or want.get("paused", False):
            stop_watchdog(ip)

    # 2) Start or adjust watchdogs for desired active targets
    for ip, t in desired.items():
        if t.get("paused", False):
            # ensure it is not running
            continue
        source_ip = t.get("source_ip")

        info = monitored_targets.get(ip)
        if info is None:
            # Not running yet -> start
            proc = start_watchdog(ip, source_ip)
            if proc:
                monitored_targets[ip] = {"proc": proc, "source_ip": source_ip, "paused": False}
            continue

        # If running, check if the source_ip changed; if so, restart with new args.
        old_src = info.get("source_ip")
        proc: subprocess.Popen = info.get("proc")
        dead = (proc is None) or (proc.poll() is not None)
        if dead:
            logger.warning(f"Watchdog for {ip} is not running; restarting.")
            proc = start_watchdog(ip, source_ip)
            if proc:
                monitored_targets[ip] = {"proc": proc, "source_ip": source_ip, "paused": False}
        elif old_src != source_ip:
            logger.info(f"Source IP changed for {ip}: {old_src} -> {source_ip}; restarting.")
            stop_watchdog(ip)
            proc = start_watchdog(ip, source_ip)
            if proc:
                monitored_targets[ip] = {"proc": proc, "source_ip": source_ip, "paused": False}

# ----------------------------
# Graceful shutdown handling
# ----------------------------
_shutdown = threading.Event()

def _handle_signal(signum, frame):
    logger.info(f"Received signal {signum}; shutting down children.")
    _shutdown.set()

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ----------------------------
# Main loop
# ----------------------------
def main() -> int:
    global settings, last_targets_mtime, last_settings_mtime

    # Initial reconcile on startup
    targets = load_targets()
    logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
    reconcile_targets(targets)

    # Determine how often the controller scans for changes (seconds).
    # If not present in YAML, default to 2 seconds.
    controller_cfg = (settings.get("controller") or {})
    scan_interval = int(controller_cfg.get("scan_interval_seconds", 2))

    while not _shutdown.is_set():
        try:
            # 1) Detect settings changes and hot-reload if mtime has changed
            curr_settings_mtime = _safe_mtime(SETTINGS_FILE)
            if curr_settings_mtime != last_settings_mtime:
                settings = load_settings(SETTINGS_FILE)
                # Re-apply the log level from YAML to this already-created logger
                refresh_logger_levels(logger, "controller", settings)
                last_settings_mtime = curr_settings_mtime
                logger.info("Settings reloaded and logging level refreshed for 'controller'.")

                # If you want to trigger any global behavior changes on settings reload, do it here.
                # For example: adjust scan_interval dynamically.
                controller_cfg = (settings.get("controller") or {})
                scan_interval = int(controller_cfg.get("scan_interval_seconds", scan_interval))
                logger.debug(f"Controller scan_interval now {scan_interval}s.")

            # 2) Detect targets changes and reconcile
            curr_targets_mtime = _safe_mtime(CONFIG_FILE)
            if curr_targets_mtime != last_targets_mtime:
                targets = load_targets()
                last_targets_mtime = curr_targets_mtime
                logger.info(f"Targets file changed; reconciling {len(targets)} targets.")
                reconcile_targets(targets)

            # 3) Reap any exited children and restart if needed (optional policy)
            for ip, info in list(monitored_targets.items()):
                proc: subprocess.Popen = info.get("proc")
                if proc and proc.poll() is not None:
                    rc = proc.returncode
                    logger.warning(f"Watchdog for {ip} exited with code {rc}; restarting.")
                    stop_watchdog(ip)
                    # Find the current desired source_ip from targets list
                    src = None
                    for t in targets:
                        if t["ip"] == ip and not t.get("paused", False):
                            src = t.get("source_ip")
                            break
                    # Restart only if still desired
                    if src is not None or any(t["ip"] == ip and not t.get("paused", False) for t in targets):
                        newp = start_watchdog(ip, src)
                        if newp:
                            monitored_targets[ip] = {"proc": newp, "source_ip": src, "paused": False}

            # 4) Sleep before next scan
            _shutdown.wait(timeout=scan_interval)

        except Exception as e:
            logger.error(f"Controller loop error: {e}")
            # brief backoff to avoid tight error loops
            time.sleep(1)

    # On shutdown: stop all children
    logger.info("Controller stopping. Terminating all child watchdogs...")
    for ip in list(monitored_targets.keys()):
        stop_watchdog(ip)

    logger.info("All children terminated. Bye.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
