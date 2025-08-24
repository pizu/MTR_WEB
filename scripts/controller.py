#!/usr/bin/env python3
"""
controller.py
=============
Top-level supervisor for the MTR_WEB project.

Responsibilities (high-level)
-----------------------------
1) Load settings + targets from the repo root YAML files:
     - mtr_script_settings.yaml
     - mtr_targets.yaml

2) Maintain exactly one running watchdog per *active* target.
   - Child script: scripts/mtr_watchdog.py
   - If a watchdog dies, restart it.
   - If a target is paused/removed, stop it.
   - If a target's source_ip changes, restart with new arg.

3) Run the reporting pipeline on schedule and on YAML changes:
       graph_generator.py → timeseries_exporter.py → html_generator.py → index_generator.py

4) Hot‑reload logging levels when settings change (no restart).

This file deliberately delegates “plumbing” to modules/controller_utils.py
so it stays small and easy to reason about.
"""

from __future__ import annotations
import os
import sys
import time
import signal
import threading

# --- Import search path so modules/ is importable under systemd and shell ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# Project root and important files (repo root = parent of scripts/)
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")
LOG_DIR       = os.path.join(REPO_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Child script path
MONITOR_SCRIPT = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")

# --- Shared utils (existing project module) ---
from modules.utils import (  # noqa: E402
    load_settings,
    setup_logger,
    refresh_logger_levels,
    resolve_all_paths,
)

# --- New controller helpers (this refactor) ---
from modules.controller_utils import (  # noqa: E402
    ControllerPolicy,
    ConfigWatcher,
    PipelineRunner,
    WatchdogManager,
    load_targets as cu_load_targets,
)

# --------------------------------------------------------------------------------------
# Controller
# --------------------------------------------------------------------------------------

class Controller:
    def __init__(self, logger, settings):
        self.logger   = logger
        self.settings = settings
        self.paths    = resolve_all_paths(self.settings)

        # Policy (loop timing, pipeline schedule, rerun on change)
        self.policy = ControllerPolicy.from_settings(self.settings, self.logger)

        # Watch what matters
        self.watcher = ConfigWatcher(settings_file=SETTINGS_FILE, targets_file=CONFIG_FILE)

        # Keep the current desired_targets cached (list of dicts)
        self.desired_targets = cu_load_targets(CONFIG_FILE, self.logger)

        # Child managers
        self.watchdogs = WatchdogManager(
            repo_root=REPO_ROOT,
            scripts_dir=SCRIPTS_DIR,
            monitor_script=MONITOR_SCRIPT,
            settings_file=SETTINGS_FILE,
            logger=self.logger,
        )

        self.pipeline = PipelineRunner(
            repo_root=REPO_ROOT,
            scripts_dir=SCRIPTS_DIR,
            settings_file=SETTINGS_FILE,
            log_dir=LOG_DIR,
            logger=self.logger,
        )

        # Pipeline schedule
        self._last_pipeline_ts = 0.0

        # Initial reconcile
        self.logger.info(f"Loaded {len(self.desired_targets)} targets from mtr_targets.yaml")
        self.watchdogs.reconcile(self.desired_targets)

    # ---------- internal helpers ----------

    def _maybe_reload_settings(self):
        """Reload settings; refresh logger levels; update policy; optionally run pipeline."""
        if self.watcher.settings_changed():
            self.settings = load_settings(SETTINGS_FILE)
            refresh_logger_levels(logger=self.logger, settings=self.settings)
            self.policy = ControllerPolicy.from_settings(self.settings, self.logger)
            self.logger.info("Settings reloaded; logger levels + controller policy refreshed.")

            if self.policy.rerun_on_change:
                self.logger.info("Running pipeline due to settings change.")
                if self.pipeline.run_all():
                    self._last_pipeline_ts = time.time()

    def _maybe_reload_targets(self):
        """Reload targets, reconcile watchdogs, optionally run pipeline."""
        if self.watcher.targets_changed():
            self.desired_targets = cu_load_targets(CONFIG_FILE, self.logger)
            self.logger.info(f"Targets changed; reconciling {len(self.desired_targets)} targets.")
            self.watchdogs.reconcile(self.desired_targets)

            if self.policy.rerun_on_change:
                self.logger.info("Running pipeline due to targets change.")
                if self.pipeline.run_all():
                    self._last_pipeline_ts = time.time()

    def _maybe_run_scheduled_pipeline(self):
        """Time-based pipeline trigger according to policy.pipeline_every_seconds."""
        now = time.time()
        if (now - self._last_pipeline_ts) >= max(5, self.policy.pipeline_every_seconds):
            self.logger.debug("Time-based pipeline trigger.")
            if self.pipeline.run_all():
                self._last_pipeline_ts = now

    # ---------- public API ----------

    def tick(self):
        """One controller loop iteration."""
        self._maybe_reload_settings()
        self._maybe_reload_targets()
        self.watchdogs.reap_and_restart(self.desired_targets)
        self._maybe_run_scheduled_pipeline()


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def main() -> int:
    # Load settings first so logging respects YAML.
    settings = load_settings(SETTINGS_FILE)
    logger = setup_logger("controller", settings=settings)
    paths = resolve_all_paths(settings)

    logger.info("Controller starting…")
    logger.info(f"Repo root   : {REPO_ROOT}")
    logger.info(f"Scripts dir : {SCRIPTS_DIR}")
    logger.info(f"RRD dir     : {paths.get('rrd')}")
    logger.info(f"HTML dir    : {paths.get('html')}")

    ctl = Controller(logger=logger, settings=settings)

    # Clean shutdown support
    stop_evt = threading.Event()

    def _sig_handler(signum, _frame):
        logger.info(f"Signal {signum} received; stopping controller…")
        stop_evt.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    # Main loop
    try:
        while not stop_evt.is_set():
            try:
                ctl.tick()
            except Exception as e:
                # Non-fatal: log and continue with a short back-off to avoid tight loop
                logger.error(f"Controller loop error: {e}")
                time.sleep(1)
            stop_evt.wait(timeout=max(1, ctl.policy.loop_seconds))
    finally:
        logger.info("Stopping all watchdogs…")
        ctl.watchdogs.stop_all()
        logger.info("Controller stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
