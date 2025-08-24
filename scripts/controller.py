#!/usr/bin/env python3
"""
controller.py
-------------
Top-level supervisor for the MTR_WEB project.

What this script does
=====================
1) Watches the two project YAML files at the repo root:
   - mtr_targets.yaml
   - mtr_script_settings.yaml

2) Ensures there is **one running child process** (mtr_watchdog.py) per *active*
   target found in mtr_targets.yaml (targets with `paused: true` are not started).

3) Periodically runs the "reporting pipeline" **in order**:
   - timeseries_exporter.py  → writes JSON bundles under <html>/data/
   - graph_generator.py      → renders PNG/SVG under <html>/graphs/
   - html_generator.py       → writes HTML pages under <html>/

This is a self-contained controller. It does not depend on helper classes from
other modules, so it can be dropped into any tree as long as the shared utils
module is available.

Quick start
-----------
$ python3 scripts/controller.py --settings /opt/scripts/MTR_WEB/mtr_script_settings.yaml

Config knobs used
-----------------
- paths.*               → resolved via modules.utils.resolve_all_paths(settings)
- logging.levels.*      → applied at runtime via modules.utils.refresh_logger_levels
- controller.*          → optional controller behavior (see DEFAULTS below)

Design notes
------------
• The controller keeps a dictionary {ip: Popen} for watchdogs and restarts any
  that die unexpectedly while the target is still desired.
• File change detection uses the mtime of the two YAML files and hot-reloads
  settings + logger levels without restarting the controller.
• Pipeline runs on a simple cadence (default 120s) and on settings reloads.

This file is documented for readers with basic Python knowledge.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

# --- Make "scripts/modules" importable whether run via systemd or shell ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# --- Imports from our shared/project modules (after sys.path tweak) ---
from modules.utils import (  # noqa: E402
    load_settings,
    resolve_all_paths,
    resolve_targets_path,
    setup_logger,
    refresh_logger_levels,
)

# ----------------------------
# Defaults and constants
# ----------------------------

DEFAULTS = {
    "loop_sleep_seconds": 1,            # idle sleep between iterations
    "pipeline_interval_seconds": 120,   # run exporters/graphs/html at this cadence
    "watchdog_restart_backoff": 2,      # seconds before restarting a crashed watchdog
}

SETTINGS_FILE_DEFAULT = os.path.join(PROJECT_ROOT, "mtr_script_settings.yaml")
TARGETS_FILE_DEFAULT = os.path.join(PROJECT_ROOT, "mtr_targets.yaml")

# ----------------------------
# Helpers
# ----------------------------

def _now_iso() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def _mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return 0.0

def _read_targets(path: str) -> List[dict]:
    """
    Load targets from YAML. The file is expected to be a list of mappings like:
      - ip: 8.8.8.8
        label: Google DNS
        paused: false

    Returns a list (possibly empty). Any malformed entries are skipped with a log.
    """
    import yaml  # local import to keep top-level tidy

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
    except FileNotFoundError:
        return []
    except Exception as e:
        logging.getLogger("controller").error("Failed to parse %s: %s", path, e)
        return []

    out: List[dict] = []
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            ip = str(row.get("ip") or "").strip()
            if not ip:
                continue
            paused = bool(row.get("paused") or False)
            out.append({"ip": ip, "label": row.get("label") or ip, "paused": paused})
    return out

# ----------------------------
# Watchdog supervisor
# ----------------------------

@dataclass
class Child:
    ip: str
    popen: subprocess.Popen
    started_at: float

class WatchdogSupervisor:
    """
    Maintains one mtr_watchdog.py child per desired IP.
    """

    def __init__(self, settings: dict, logs_dir: str, logger: logging.Logger) -> None:
        self.settings = settings
        self.logs_dir = logs_dir
        self.logger = logger
        self.children: Dict[str, Child] = {}
        self.desired: Set[str] = set()
        self.backoff = int(self._get("controller.watchdog_restart_backoff",
                                     DEFAULTS["watchdog_restart_backoff"]))

    def _get(self, dotted_key: str, default):
        # dotted access into settings
        cur = self.settings
        for part in dotted_key.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur

    def set_settings(self, settings: dict) -> None:
        self.settings = settings
        # Update backoff in case it changed
        self.backoff = int(self._get("controller.watchdog_restart_backoff",
                                     DEFAULTS["watchdog_restart_backoff"]))

    def set_desired(self, ips: Iterable[str]) -> None:
        self.desired = set(ips)

    def sync(self) -> None:
        """
        Make reality match self.desired:
        • start missing
        • stop extra
        • restart crashed ones
        """
        # Start missing
        for ip in sorted(self.desired):
            if ip not in self.children:
                self._start(ip)

        # Stop extra
        for ip in list(self.children.keys()):
            if ip not in self.desired:
                self._stop(ip)

        # Restart crashed
        for ip, child in list(self.children.items()):
            if child.popen.poll() is not None:  # process exited
                rc = child.popen.returncode
                self.logger.warning("Watchdog for %s exited rc=%s; restarting if still desired.", ip, rc)
                self._stop(ip, already_dead=True)
                if ip in self.desired:
                    time.sleep(self.backoff)
                    self._start(ip)

    def stop_all(self) -> None:
        for ip in list(self.children.keys()):
            self._stop(ip)

    def _start(self, ip: str) -> None:
        cmd = [
            sys.executable,
            os.path.join(SCRIPTS_DIR, "mtr_watchdog.py"),
            "--target", ip,
            "--settings", SETTINGS_FILE,  # use current global path
        ]
        log_path = os.path.join(self.logs_dir, f"watchdog_{ip.replace('.', '_')}.log")
        self.logger.info("Started watchdog for %s (log: %s) args=%s", ip, log_path, cmd)
        lf = open(log_path, "a", buffering=1, encoding="utf-8")
        lf.write(f"{_now_iso()} START {os.path.basename(cmd[1])} {ip}\n")
        pop = subprocess.Popen(cmd, stdout=lf, stderr=lf)
        self.children[ip] = Child(ip=ip, popen=pop, started_at=time.time())

    def _stop(self, ip: str, already_dead: bool = False) -> None:
        child = self.children.pop(ip, None)
        if not child:
            return
        if already_dead:
            self.logger.info("Watchdog %s already exited.", ip)
            return
        self.logger.info("Stopping watchdog for %s …", ip)
        try:
            child.popen.terminate()
            try:
                child.popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.logger.warning("Watchdog %s did not exit; killing.", ip)
                child.popen.kill()
        except Exception as e:
            self.logger.error("Error stopping watchdog %s: %s", ip, e)

# ----------------------------
# Pipeline runner
# ----------------------------

class Pipeline:
    """
    Runs the three reporting scripts in sequence and writes a per-phase pipeline log.
    """

    def __init__(self, logs_dir: str, logger: logging.Logger) -> None:
        self.logs_dir = logs_dir
        self.logger = logger

    def run_once(self) -> None:
        phases = [
            ("timeseries_exporter.py", []),
            ("graph_generator.py", []),
            ("html_generator.py", []),
        ]
        for script, extra_args in phases:
            self._run_phase(script, extra_args)

    def _run_phase(self, script: str, extra_args: List[str]) -> None:
        script_path = os.path.join(SCRIPTS_DIR, script)
        log_path = os.path.join(self.logs_dir, f"pipeline_{script}.log")
        cmd = [sys.executable, script_path, "--settings", SETTINGS_FILE, *extra_args]

        # Short console note and a header inside the pipeline log
        self.logger.info("[pipeline] Running %s …  (log: %s)", script, log_path)
        with open(log_path, "a", buffering=1, encoding="utf-8") as lf:
            lf.write(f"\n=== {_now_iso()} | START {script} ===\n")
            lf.write(f"$ {' '.join(cmd)}\n")
            rc = 0
            try:
                proc = subprocess.Popen(cmd, stdout=lf, stderr=lf)
                rc = proc.wait()
            except Exception as e:
                lf.write(f"[controller] Failed starting {script}: {e}\n")
                rc = 1

        # Summarise to controller log, and tail any error from the phase log on failure
        if rc == 0:
            self.logger.info("[pipeline] %s OK", script)
        else:
            self.logger.error("[pipeline] %s failed with rc=%d", script, rc)
            try:
                tail = self._tail_file(log_path, 25)
                if tail.strip():
                    self.logger.error("[pipeline] --- tail of %s ---\n%s\n[pipeline] --- end tail ---",
                                      log_path, tail)
            except Exception:
                pass

    @staticmethod
    def _tail_file(path: str, lines: int) -> str:
        """Return the last N lines of a text file."""
        data = []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                data.append(line.rstrip("\n"))
        return "\n".join(data[-lines:])

# ----------------------------
# Main
# ----------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="MTR_WEB controller (supervisor)")
    ap.add_argument("--settings", default=SETTINGS_FILE_DEFAULT,
                    help="Path to mtr_script_settings.yaml (default: %(default)s)")
    return ap.parse_args(argv)

def main(argv: Optional[List[str]] = None) -> int:
    global SETTINGS_FILE  # used by WatchdogSupervisor/Pipeline to re-use current path
    args = parse_args(argv)
    SETTINGS_FILE = os.path.abspath(args.settings)

    # 1) Load settings + resolve paths
    settings = load_settings(SETTINGS_FILE)
    paths = resolve_all_paths(settings)

    # 2) Logging
    logger = setup_logger("controller", settings=settings)
    # Apply runtime levels immediately (2-arg form: registry, settings)
    refresh_logger_levels({"controller": logger}, settings)

    logs_dir = paths.get("logs") or os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # 3) Note schema
    schema = settings.get("rrd", {}).get("data_sources")
    if isinstance(schema, list):
        names = ", ".join(str(s.get("name")) for s in schema if isinstance(s, dict) and s.get("name"))
        if names:
            logger.info("Schema metrics: %s", names)

    # 4) Setup helpers
    pipeline_interval = int(settings.get("controller", {}).get(
        "pipeline_interval_seconds", DEFAULTS["pipeline_interval_seconds"]))
    loop_sleep = int(settings.get("controller", {}).get(
        "loop_sleep_seconds", DEFAULTS["loop_sleep_seconds"]))

    watchdogs = WatchdogSupervisor(settings=settings, logs_dir=logs_dir, logger=logger)
    pipeline = Pipeline(logs_dir=logs_dir, logger=logger)

    # 5) Initial target set + mtimes
    targets_file = resolve_targets_path(settings) or TARGETS_FILE_DEFAULT
    settings_mtime = _mtime(SETTINGS_FILE)
    targets_mtime = _mtime(targets_file)

    desired_ips = [t["ip"] for t in _read_targets(targets_file) if not t.get("paused")]
    watchdogs.set_desired(desired_ips)
    watchdogs.sync()  # start initial set

    last_pipeline = 0.0

    logger.info("Controller started. Watching settings: %s and targets: %s", SETTINGS_FILE, targets_file)

    # 6) Main loop
    try:
        while True:
            try:
                # Reload settings if changed
                sm = _mtime(SETTINGS_FILE)
                if sm != settings_mtime:
                    settings = load_settings(SETTINGS_FILE)
                    paths = resolve_all_paths(settings)
                    refresh_logger_levels({"controller": logger}, settings)
                    watchdogs.set_settings(settings)
                    pipeline_interval = int(settings.get("controller", {}).get(
                        "pipeline_interval_seconds", DEFAULTS["pipeline_interval_seconds"]))
                    loop_sleep = int(settings.get("controller", {}).get(
                        "loop_sleep_seconds", DEFAULTS["loop_sleep_seconds"]))
                    settings_mtime = sm
                    logger.info("Settings reloaded; 'controller' log level refreshed.")

                # Reload targets if changed
                tf = resolve_targets_path(settings) or targets_file
                if tf != targets_file:
                    targets_file = tf
                    targets_mtime = 0.0  # force a reload next block
                tm = _mtime(targets_file)
                if tm != targets_mtime:
                    targets_mtime = tm
                    desired_ips = [t["ip"] for t in _read_targets(targets_file) if not t.get("paused")]
                    watchdogs.set_desired(desired_ips)

                # Ensure watchdogs match desired state (and restart crashed ones)
                watchdogs.sync()

                # Run pipeline on cadence
                now = time.time()
                if (now - last_pipeline) >= pipeline_interval:
                    pipeline.run_once()
                    last_pipeline = now

                time.sleep(loop_sleep)
            except Exception as e:
                # Non-fatal: log and continue with a short back-off to avoid tight loop
                logger.error("Controller loop error: %s", e)
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt, shutting down.")
    finally:
        # Stop all watchdogs
        logger.info("Stopping all watchdogs…")
        watchdogs.stop_all()
        logger.info("Controller stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
