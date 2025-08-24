#!/usr/bin/env python3
"""
controller.py
=============

Role
----
Project-wide supervisor for MTR_WEB. It:
  1) Watches config files:
       - mtr_script_settings.yaml
       - mtr_targets.yaml
  2) Ensures **one** running mtr_watchdog.py per active target (from mtr_targets.yaml).
  3) Runs the "pipeline" scripts on a cadence **and** when configs change.

What counts as the "pipeline"?
------------------------------
By default we try to run these, **in order** (skipping any missing files):
  1) rrd_exporter.py        (keeps meta/settings RRDs aligned with YAML)
  2) timeseries_exporter.py (exports JSON time series into <html>/data/)
  3) graph_generator.py     (renders per-hop graphs into <html>/graphs/)
  4) html_generator.py      (renders target pages into <html>/)
  5) index_generator.py     (renders index/home page into <html>/)

You can override order and/or pass extra args from mtr_script_settings.yaml:

controller:
  pipeline:
    - rrd_exporter.py
    - timeseries_exporter.py
    - graph_generator.py
    - html_generator.py
    - index_generator.py
  pipeline_extra_args:
    graph_generator.py: ["--summaries", "yes"]
  pipeline_interval_seconds: 120
  loop_sleep_seconds: 1
  watchdog_restart_backoff: 2

Basic usage
-----------
$ python3 scripts/controller.py --settings /opt/scripts/MTR_WEB/mtr_script_settings.yaml

Requirements
------------
• This script expects to live in /opt/scripts/MTR_WEB/scripts/ alongside the
  other Python scripts it launches.
• The shared helpers are imported from scripts/modules/utils.py.

Notes
-----
• If a pipeline phase script is not present (file missing), it is logged as
  "skipped (not found)" and the rest continues.
• When settings or targets change on disk, the controller:
    - Reloads settings and reapplies logger levels,
    - Synchronizes watchdogs,
    - Kicks a *one-off* pipeline run immediately (in addition to cadence).
• All file paths are resolved via modules.utils.resolve_all_paths(settings).

This file is documented for users with basic Python knowledge.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set

# --- Layout and import path tweaks ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# --- Project helpers (after sys.path amended) ---
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

SETTINGS_FILE_DEFAULT = os.path.join(PROJECT_ROOT, "mtr_script_settings.yaml")
TARGETS_FILE_DEFAULT = os.path.join(PROJECT_ROOT, "mtr_targets.yaml")

DEFAULTS = {
    "loop_sleep_seconds": 1,
    "pipeline_interval_seconds": 120,
    "watchdog_restart_backoff": 2,
    # Default pipeline order; missing scripts are skipped gracefully.
    "pipeline": [
        "rrd_exporter.py",
        "timeseries_exporter.py",
        "graph_generator.py",
        "html_generator.py",
        "index_generator.py",
    ],
    # Optional per-phase extra args
    "pipeline_extra_args": {},
}


def _now_iso() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return 0.0


def _read_targets(path: str) -> List[dict]:
    """
    Parse mtr_targets.yaml. Each entry should look like:
      - ip: 8.8.8.8
        label: Google
        paused: false
    Returns only entries with a non-empty 'ip'. Non-dict rows are ignored.
    """
    import yaml  # local to keep imports tidy
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
    Keeps exactly one `mtr_watchdog.py` child process per desired IP and restarts
    crashed ones while still desired.
    """

    def __init__(self, settings: dict, logs_dir: str, logger: logging.Logger):
        self.settings = settings
        self.logs_dir = logs_dir
        self.logger = logger
        self.children: Dict[str, Child] = {}
        self.backoff = int(self._get("controller.watchdog_restart_backoff",
                                     DEFAULTS["watchdog_restart_backoff"]))

    def _get(self, dotted_key: str, default):
        cur = self.settings
        for part in dotted_key.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur

    def set_settings(self, settings: dict) -> None:
        self.settings = settings
        self.backoff = int(self._get("controller.watchdog_restart_backoff",
                                     DEFAULTS["watchdog_restart_backoff"]))

    def set_desired(self, ips: Iterable[str]) -> None:
        self.desired: Set[str] = set(ips)

    def sync(self) -> None:
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
            if child.popen.poll() is not None:
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
            "--settings", SETTINGS_FILE,
        ]
        os.makedirs(self.logs_dir, exist_ok=True)
        log_path = os.path.join(self.logs_dir, f"watchdog_{ip.replace('.', '_')}.log")
        self.logger.info("Started watchdog for %s (log: %s) args=%s", ip, log_path, cmd)
        lf = open(log_path, "a", buffering=1, encoding="utf-8")
        lf.write(f"{_now_iso()} START mtr_watchdog.py {ip}\n")
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
    Runs a sequence of project scripts, writing a per-phase pipeline log.

    The list of phases and their extra args come from:
      * settings['controller']['pipeline'] (fallback to DEFAULTS['pipeline'])
      * settings['controller']['pipeline_extra_args'] (optional)
    Scripts that do not exist are logged as "skipped (not found)".
    """

    def __init__(self, settings: dict, logs_dir: str, logger: logging.Logger) -> None:
        self.settings = settings
        self.logs_dir = logs_dir
        self.logger = logger

    def set_settings(self, settings: dict) -> None:
        self.settings = settings

    def _phases(self) -> List[tuple[str, Sequence[str]]]:
        ctl = self.settings.get("controller", {}) if isinstance(self.settings, dict) else {}
        order = ctl.get("pipeline") or DEFAULTS["pipeline"]
        extra = ctl.get("pipeline_extra_args") or {}
        phases: List[tuple[str, Sequence[str]]] = []
        for script in order:
            args = extra.get(script) or []
            if not isinstance(args, (list, tuple)):
                args = [str(args)]
            phases.append((script, list(args)))
        return phases

    def run_once(self) -> None:
        for script, extra_args in self._phases():
            self._run_phase(script, list(extra_args))

    def _run_phase(self, script: str, extra_args: List[str]) -> None:
        script_path = os.path.join(SCRIPTS_DIR, script)
        log_path = os.path.join(self.logs_dir, f"pipeline_{script}.log")
        if not os.path.isfile(script_path):
            self.logger.info("[pipeline] %s skipped (not found).", script)
            return

        cmd = [sys.executable, script_path, "--settings", SETTINGS_FILE, *extra_args]
        self.logger.info("[pipeline] Running %s …  (log: %s)", script, log_path)

        os.makedirs(self.logs_dir, exist_ok=True)
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
        data = []
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    data.append(line.rstrip("\n"))
        except FileNotFoundError:
            return ""
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
    global SETTINGS_FILE  # used by WatchdogSupervisor/Pipeline
    args = parse_args(argv)
    SETTINGS_FILE = os.path.abspath(args.settings)

    # Load settings and resolve paths
    settings = load_settings(SETTINGS_FILE)
    paths = resolve_all_paths(settings)

    # Logger and dynamic levels
    logger = setup_logger("controller", settings=settings)
    refresh_logger_levels({"controller": logger}, settings)

    logs_dir = paths.get("logs") or os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Pipeline and loop cadence
    ctl = settings.get("controller", {}) if isinstance(settings, dict) else {}
    pipeline_interval = int(ctl.get("pipeline_interval_seconds", DEFAULTS["pipeline_interval_seconds"]))
    loop_sleep = int(ctl.get("loop_sleep_seconds", DEFAULTS["loop_sleep_seconds"]))

    # Announce schema (from settings.rrd.data_sources)
    schema = settings.get("rrd", {}).get("data_sources")
    if isinstance(schema, list):
        names = ", ".join(str(s.get("name")) for s in schema if isinstance(s, dict) and s.get("name"))
        if names:
            logger.info("Schema metrics: %s", names)

    # Prepare helpers
    pipeline = Pipeline(settings=settings, logs_dir=logs_dir, logger=logger)
    watchdogs = WatchdogSupervisor(settings=settings, logs_dir=logs_dir, logger=logger)

    # Determine config file paths and mtimes
    targets_file = resolve_targets_path(settings) or TARGETS_FILE_DEFAULT
    settings_mtime = _mtime(SETTINGS_FILE)
    targets_mtime = _mtime(targets_file)

    # Initial desired targets → start watchdogs
    desired_ips = [t["ip"] for t in _read_targets(targets_file) if not t.get("paused")]
    watchdogs.set_desired(desired_ips)
    watchdogs.sync()

    last_pipeline = 0.0
    run_pipeline_now = True  # run once at startup

    logger.info("Controller started. Watching settings: %s and targets: %s", SETTINGS_FILE, targets_file)

    try:
        while True:
            try:
                # Detect settings change
                sm = _mtime(SETTINGS_FILE)
                if sm != settings_mtime:
                    settings = load_settings(SETTINGS_FILE)
                    paths = resolve_all_paths(settings)
                    refresh_logger_levels({"controller": logger}, settings)
                    pipeline.set_settings(settings)
                    watchdogs.set_settings(settings)
                    ctl = settings.get("controller", {}) if isinstance(settings, dict) else {}
                    pipeline_interval = int(ctl.get("pipeline_interval_seconds",
                                                    DEFAULTS["pipeline_interval_seconds"]))
                    loop_sleep = int(ctl.get("loop_sleep_seconds", DEFAULTS["loop_sleep_seconds"]))
                    settings_mtime = sm
                    run_pipeline_now = True  # kick a pipeline run after settings reload
                    logger.info("Settings reloaded; 'controller' log level refreshed.")

                # Detect targets path change (resolve may point to a different file now)
                new_targets_file = resolve_targets_path(settings) or targets_file
                if new_targets_file != targets_file:
                    targets_file = new_targets_file
                    targets_mtime = 0.0  # force reload below

                # Detect targets content change
                tm = _mtime(targets_file)
                if tm != targets_mtime:
                    targets_mtime = tm
                    desired_ips = [t["ip"] for t in _read_targets(targets_file) if not t.get("paused")]
                    watchdogs.set_desired(desired_ips)
                    run_pipeline_now = True  # target set changed → refresh exported data/HTML

                # Keep watchdogs aligned (and restart crashed)
                watchdogs.sync()

                # Pipeline cadence or forced run
                now = time.time()
                if run_pipeline_now or (now - last_pipeline) >= pipeline_interval:
                    pipeline.run_once()
                    last_pipeline = now
                    run_pipeline_now = False

                time.sleep(loop_sleep)
            except Exception as e:
                # Non-fatal; log and keep going with small backoff
                logger.error("Controller loop error: %s", e)
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt, shutting down.")
    finally:
        logger.info("Stopping all watchdogs…")
        watchdogs.stop_all()
        logger.info("Controller stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
