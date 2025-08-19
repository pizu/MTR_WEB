#!/usr/bin/env python3
"""
controller.py
-------------
Top-level supervisor that:
- Watches mtr_targets.yaml + mtr_script_settings.yaml
- Ensures one mtr_watchdog.py per active target (start/stop/restart)
- Periodically runs the reporting pipeline:
    graph_generator.py → timeseries_exporter.py → html_generator.py → index_generator.py
- Hot-reloads logging levels without restart

USAGE:
    python3 scripts/controller.py

SYSTEMD:
    WorkingDirectory=/opt/scripts/MTR_WEB
    ExecStart=/usr/bin/python3 scripts/controller.py
"""

import os
import sys
import time
import yaml
import signal
import threading
from typing import Dict, List, Optional

# ----------------------------
# Import shared helpers
# ----------------------------
SCRIPTS_DIR  = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR  = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

from modules.utils import load_settings, setup_logger, refresh_logger_levels  # noqa: E402
from modules.controller_utils import WatchdogManager                     # noqa: E402
from modules.pipeline_utils import PipelineRunner                        # noqa: E402


# ----------------------------
# Constants / Paths
# ----------------------------
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")

MONITOR_SCRIPT         = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")
GRAPH_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT     = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")
HTML_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "index_generator.py")

# ----------------------------
# Helpers
# ----------------------------
def _safe_mtime(path: str) -> float:
    """Return mtime (float seconds) or 0.0 if missing/failed."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

def _load_targets(logger) -> List[Dict]:
    """
    Parse mtr_targets.yaml → normalized list:
    {ip, description, source_ip, paused}
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
# Main
# ----------------------------
def main() -> int:
    # Load settings first (controls logging level)
    settings = load_settings(SETTINGS_FILE)

    # Logger: honors logging_levels.controller in YAML
    logger = setup_logger(
        "controller",
        settings.get("log_directory", "/tmp"),
        "controller.log",
        settings=settings
    )

    # Managers
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

    # Scan interval + pipeline run policy (from YAML)
    controller_cfg = (settings.get("controller") or {})
    scan_interval = int(controller_cfg.get("scan_interval_seconds", 2))     # how often to rescan files
    pipeline_enabled = bool(controller_cfg.get("pipeline_enabled", True))    # master on/off
    pipeline_every = int(controller_cfg.get("pipeline_run_every_seconds", 60))  # periodic schedule
    pipeline_on_change = bool(controller_cfg.get("pipeline_run_on_change", True))  # run when cfg/targets change

    last_targets_mtime  = _safe_mtime(CONFIG_FILE)
    last_settings_mtime = _safe_mtime(SETTINGS_FILE)
    last_pipeline_ts    = 0.0  # last successful pipeline run (epoch seconds)

    # Initial bring-up
    targets = _load_targets(logger)
    logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
    watchdogs.reconcile(targets)

    # Shutdown handling
    stop_evt = threading.Event()
    def _sig_handler(signum, _frame):
        logger.info(f"Signal {signum} received; stopping controller…")
        stop_evt.set()
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    # Main loop
    while not stop_evt.is_set():
        try:
            # Settings hot-reload
            curr_settings_mtime = _safe_mtime(SETTINGS_FILE)
            if curr_settings_mtime != last_settings_mtime:
                settings = load_settings(SETTINGS_FILE)
                refresh_logger_levels(logger, "controller", settings)
                last_settings_mtime = curr_settings_mtime
                logger.info("Reloaded settings and refreshed logging level.")
                # Re-pull controller policy
                controller_cfg = (settings.get("controller") or {})
                scan_interval = int(controller_cfg.get("scan_interval_seconds", scan_interval))
                pipeline_enabled = bool(controller_cfg.get("pipeline_enabled", pipeline_enabled))
                pipeline_every = int(controller_cfg.get("pipeline_run_every_seconds", pipeline_every))
                pipeline_on_change = bool(controller_cfg.get("pipeline_run_on_change", pipeline_on_change))
                logger.debug(
                    f"controller: scan={scan_interval}s, pipeline_enabled={pipeline_enabled}, "
                    f"every={pipeline_every}s, on_change={pipeline_on_change}"
                )
                if pipeline_enabled and pipeline_on_change:
                    logger.info("Running pipeline (settings changed).")
                    ok = pipeline.run_all()
                    if ok:
                        last_pipeline_ts = time.time()

            # Targets hot-reload
            curr_targets_mtime = _safe_mtime(CONFIG_FILE)
            if curr_targets_mtime != last_targets_mtime:
                targets = _load_targets(logger)
                last_targets_mtime = curr_targets_mtime
                logger.info(f"Targets changed; reconciling {len(targets)} targets.")
                watchdogs.reconcile(targets)
                if pipeline_enabled and pipeline_on_change:
                    logger.info("Running pipeline (targets changed).")
                    ok = pipeline.run_all()
                    if ok:
                        last_pipeline_ts = time.time()

            # Periodic pipeline scheduler
            if pipeline_enabled:
                now = time.time()
                if (now - last_pipeline_ts) >= max(5, pipeline_every):
                    logger.debug("Time-based pipeline trigger.")
                    ok = pipeline.run_all()
                    if ok:
                        last_pipeline_ts = now

            # Reap/restart dead watchdogs (policy: always keep them up)
            watchdogs.reap_and_restart(desired_targets=targets)

            # Sleep before next scan
            stop_evt.wait(timeout=scan_interval)

        except Exception as e:
            logger.error(f"Controller loop error: {e}")
            time.sleep(1)

    # Shutdown
    logger.info("Stopping all watchdogs…")
    watchdogs.stop_all()
    logger.info("Controller stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
