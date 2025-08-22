#!/usr/bin/env python3
"""
controller.py
-------------
Top‑level supervisor for the MTR_WEB project.

WHAT THIS SCRIPT DOES
=====================
1) Watches the two project YAML files at the repo root:
   - mtr_targets.yaml
   - mtr_script_settings.yaml

2) Ensures there is ONE running child process (mtr_watchdog.py) per *active*
   target found in mtr_targets.yaml (targets with `paused: true` are not started).

3) Periodically runs the "reporting pipeline" in order:
     graph_generator.py  →  timeseries_exporter.py  →  html_generator.py  →  index_generator.py
   The pipeline can also run immediately when YAML files change (configurable).

4) Hot‑reloads logging levels at runtime when mtr_script_settings.yaml changes
   (no controller restart required). Uses modules.utils.setup_logger/refresh_logger_levels.

USAGE
=====
    python3 scripts/controller.py

SYSTEMD EXAMPLE
===============
[Unit]
Description=MTR WEB Monitoring Controller
After=network.target

[Service]
WorkingDirectory=/opt/scripts/MTR_WEB
ExecStart=/usr/bin/python3 scripts/controller.py
Restart=always
User=root
Group=root

[Install]
WantedBy=multi-user.target
"""

import os
import sys
import time
import yaml
import signal
import threading
from typing import Dict, List, Optional
from modules.utils import load_settings, setup_logger, refresh_logger_levels, resolve_all_paths  # add resolve_all_paths

# --- Make "scripts/modules" importable whether run via systemd or shell ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# --- Imports from our shared/project modules ---
from modules.utils import load_settings, setup_logger, refresh_logger_levels  # noqa: E402
from modules.controller_utils import WatchdogManager                          # noqa: E402
from modules.pipeline_utils import PipelineRunner                             # noqa: E402


# ----------------------------
# Paths (absolute)
# ----------------------------
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))  # repo root (one level above scripts/)
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")            # targets list
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")    # global settings

# Child script paths (absolute)
MONITOR_SCRIPT         = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")
GRAPH_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT     = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")
HTML_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "index_generator.py")


# ----------------------------
# Small utilities (local)
# ----------------------------
def _safe_mtime(path: str) -> float:
    """Return file modification time in seconds since epoch, or 0.0 if missing/inaccessible."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


def _load_targets(logger) -> List[Dict]:
    """
    Read mtr_targets.yaml and normalize entries into:
      { "ip": str, "description": str, "source_ip": Optional[str], "paused": bool }
    Any rows missing 'ip' are ignored.
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


def _read_controller_policy(settings: Dict, logger) -> Dict:
    """
    Pull controller policy from YAML with backward‑compatible keys.

    Recognized keys (preferred):
      controller.loop_seconds
      controller.pipeline_every_seconds
      controller.rerun_pipeline_on_changes

    Backward‑compatible fallbacks if present:
      controller.scan_interval_seconds (alias of loop_seconds)
      controller.pipeline_run_every_seconds (alias of pipeline_every_seconds)
      controller.pipeline_run_on_change (alias of rerun_pipeline_on_changes)
    """
    cfg = (settings.get("controller") or {})

    # Prefer new keys; fall back to older aliases if present.
    loop_seconds = int(cfg.get("loop_seconds",
                        cfg.get("scan_interval_seconds", 2)))  # default 2s

    pipeline_every_seconds = int(cfg.get("pipeline_every_seconds",
                                  cfg.get("pipeline_run_every_seconds", 60)))  # default 60s

    rerun_on_change = bool(cfg.get("rerun_pipeline_on_changes",
                             cfg.get("pipeline_run_on_change", True)))  # default True

    logger.debug(f"controller policy: loop_seconds={loop_seconds}, "
                 f"pipeline_every_seconds={pipeline_every_seconds}, "
                 f"rerun_on_change={rerun_on_change}")

    return {
        "loop_seconds": loop_seconds,
        "pipeline_every_seconds": pipeline_every_seconds,
        "rerun_on_change": rerun_on_change,
    }


# ----------------------------
# Main entrypoint
# ----------------------------
def main() -> int:
    # 1) Load settings (controls logging behavior) and create the 'controller' logger
    settings = load_settings(SETTINGS_FILE)
    paths = resolve_all_paths(settings)
    logger = setup_logger("controller", settings=settings)

    # 2) Instantiate helpers
    watchdogs = WatchdogManager(
        repo_root=REPO_ROOT,
        monitor_script=MONITOR_SCRIPT,
        settings_file=SETTINGS_FILE,
        logger=logger
    )
    pipeline = PipelineRunner(
        repo_root=REPO_ROOT,
        scripts=[
            GRAPH_GENERATOR_SCRIPT,
            TS_EXPORTER_SCRIPT,
            HTML_GENERATOR_SCRIPT,
            INDEX_GENERATOR_SCRIPT,
        ],
        settings_file=SETTINGS_FILE,
        logger=logger
    )

    # 3) Initial state
    policy = _read_controller_policy(settings, logger)
    loop_seconds = policy["loop_seconds"]
    pipeline_every_seconds = policy["pipeline_every_seconds"]
    rerun_on_change = policy["rerun_on_change"]

    last_targets_mtime  = _safe_mtime(CONFIG_FILE)
    last_settings_mtime = _safe_mtime(SETTINGS_FILE)
    last_pipeline_ts    = 0.0

    targets = _load_targets(logger)
    logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
    watchdogs.reconcile(targets)

    # 4) Clean shutdown support
    stop_evt = threading.Event()

    def _sig_handler(signum, _frame):
        logger.info(f"Signal {signum} received; stopping controller…")
        stop_evt.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    # 5) Main loop
    while not stop_evt.is_set():
        try:
            # Settings hot‑reload
            curr_settings_mtime = _safe_mtime(SETTINGS_FILE)
            if curr_settings_mtime != last_settings_mtime:
                settings = load_settings(SETTINGS_FILE)
                refresh_logger_levels(logger, "controller", settings)
                last_settings_mtime = curr_settings_mtime
                logger.info("Settings reloaded; 'controller' log level refreshed.")

                # Apply any policy changes at runtime
                policy = _read_controller_policy(settings, logger)
                loop_seconds = policy["loop_seconds"]
                pipeline_every_seconds = policy["pipeline_every_seconds"]
                rerun_on_change = policy["rerun_on_change"]

                if rerun_on_change:
                    logger.info("Running pipeline due to settings change.")
                    if pipeline.run_all():
                        last_pipeline_ts = time.time()

            # Targets file hot‑reload
            curr_targets_mtime = _safe_mtime(CONFIG_FILE)
            if curr_targets_mtime != last_targets_mtime:
                targets = _load_targets(logger)
                last_targets_mtime = curr_targets_mtime
                logger.info(f"Targets changed; reconciling {len(targets)} targets.")
                watchdogs.reconcile(targets)
                if rerun_on_change:
                    logger.info("Running pipeline due to targets change.")
                    if pipeline.run_all():
                        last_pipeline_ts = time.time()

            # Periodic pipeline schedule
            now = time.time()
            if (now - last_pipeline_ts) >= max(5, pipeline_every_seconds):
                logger.debug("Time‑based pipeline trigger.")
                if pipeline.run_all():
                    last_pipeline_ts = now

            # Reap/restart dead watchdogs
            watchdogs.reap_and_restart(desired_targets=targets)

            # Idle wait
            stop_evt.wait(timeout=loop_seconds)

        except Exception as e:
            # Non‑fatal: log and continue with a short back‑off to avoid tight loop
            logger.error(f"Controller loop error: {e}")
            time.sleep(1)

    # 6) Shutdown: stop all watchdogs
    logger.info("Stopping all watchdogs…")
    watchdogs.stop_all()
    logger.info("Controller stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
