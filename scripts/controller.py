#!/usr/bin/env python3
"""
controller.py
=============
Top-level supervisor for the MTR_WEB project.

What this file does
-------------------
1) Watches the two project YAML files at the repo root:
     - mtr_targets.yaml
     - mtr_script_settings.yaml

2) Ensures there is **exactly one** running child process (mtr_watchdog.py) per
   *active* target found in mtr_targets.yaml (`paused: true` targets are not started).
   If a watchdog dies, it gets restarted automatically. If a target is removed or
   paused, its watchdog is stopped.

3) Periodically runs the “reporting pipeline” in order:
       graph_generator.py  →  timeseries_exporter.py  →  html_generator.py  →  index_generator.py
   The pipeline also runs immediately when either YAML changes (configurable).

4) Hot-reloads logging levels when mtr_script_settings.yaml changes via
   modules.utils.refresh_logger_levels(logger=..., settings=...). No controller restart needed.

How logging works here
----------------------
- We use modules.utils.setup_logger("controller", settings=settings).
- The controller writes human messages to logs/controller.log.
- Each pipeline stage gets its own rotating log in logs/pipeline_*.log (stdout/stderr captured).

Config keys used (with safe fallbacks)
--------------------------------------
controller.loop_seconds               (default 2s)
controller.pipeline_every_seconds     (default 60s)
controller.rerun_pipeline_on_changes  (default True)

Backward-compatible aliases (if you still have them in YAML):
- controller.scan_interval_seconds           -> loop_seconds
- controller.pipeline_run_every_seconds      -> pipeline_every_seconds
- controller.pipeline_run_on_change          -> rerun_pipeline_on_changes

Child processes and paths
-------------------------
- Spawns:  /usr/bin/python3 scripts/mtr_watchdog.py --target <IP> [--source <SRC>] --settings <path>
- Ensures PYTHONPATH contains the scripts/ dir so that child scripts can import modules.*
  (*Important when running via systemd.)

Systemd example
---------------
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

from __future__ import annotations
import os
import sys
import time
import yaml
import shlex
import signal
import threading
import subprocess
from typing import Dict, List, Optional

# --- Make scripts/modules importable whether run via systemd or shell ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# Project root and important files
REPO_ROOT     = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
CONFIG_FILE   = os.path.join(REPO_ROOT, "mtr_targets.yaml")          # targets list
SETTINGS_FILE = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")  # global settings
LOG_DIR       = os.path.join(REPO_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Child script paths (absolute)
MONITOR_SCRIPT         = os.path.join(SCRIPTS_DIR, "mtr_watchdog.py")
GRAPH_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "graph_generator.py")
TS_EXPORTER_SCRIPT     = os.path.join(SCRIPTS_DIR, "timeseries_exporter.py")
HTML_GENERATOR_SCRIPT  = os.path.join(SCRIPTS_DIR, "html_generator.py")
INDEX_GENERATOR_SCRIPT = os.path.join(SCRIPTS_DIR, "index_generator.py")

# --- Imports from shared utils (after sys.path was fixed) ---
from modules.utils import (  # noqa: E402
    load_settings,
    setup_logger,
    refresh_logger_levels,
    resolve_all_paths,
)

# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
def _safe_mtime(path: str) -> float:
    """Return file modification time or 0.0 if missing/inaccessible."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


def _load_targets(logger) -> List[Dict]:
    """
    Parse mtr_targets.yaml into normalized list items:

      {
        "ip": "8.8.8.8",
        "description": "Google DNS",
        "source_ip": "192.0.2.10"  # optional (can be 'source' or 'source_ip' in YAML)
        "paused": false
      }

    Missing/blank 'ip' rows are ignored.
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
                "source_ip": (t.get("source_ip") or t.get("source")) or None,
                "paused": bool(t.get("paused", False)),
            })
        return out
    except Exception as e:
        logger.error(f"Failed to read {CONFIG_FILE}: {e}")
        return []


def _read_policy(settings: Dict, logger) -> Dict:
    """
    Pull controller policy with backward-compatible keys.
    """
    cfg = (settings.get("controller") or {})
    loop_seconds = int(cfg.get("loop_seconds", cfg.get("scan_interval_seconds", 2)))
    pipeline_every_seconds = int(cfg.get("pipeline_every_seconds",
                                         cfg.get("pipeline_run_every_seconds", 60)))
    rerun_on_change = bool(cfg.get("rerun_pipeline_on_changes",
                                   cfg.get("pipeline_run_on_change", True)))
    logger.debug(
        f"controller policy: loop_seconds={loop_seconds}, "
        f"pipeline_every_seconds={pipeline_every_seconds}, "
        f"rerun_on_change={rerun_on_change}"
    )
    return {
        "loop_seconds": loop_seconds,
        "pipeline_every_seconds": pipeline_every_seconds,
        "rerun_on_change": rerun_on_change,
    }


def _child_env() -> Dict[str, str]:
    """
    Return environment for child processes.
    Ensures PYTHONPATH contains scripts/ so module imports work everywhere.
    """
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    paths = [SCRIPTS_DIR]
    if pp:
        paths.append(pp)
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def _start_watchdog(ip: str, settings_file: str, source_ip: Optional[str], logger) -> subprocess.Popen:
    """
    Spawn one watchdog for one IP.
    The watchdog script itself decides which monitor entrypoint to use based on YAML.
    """
    cmd = ["/usr/bin/python3", MONITOR_SCRIPT, "--target", ip, "--settings", settings_file]
    if source_ip:
        # mtr_watchdog.py we shipped accepts '--source' (kept BC with '--source_ip' too).
        cmd.extend(["--source", str(source_ip)])

    logger.info(f"Starting watchdog for {ip}  args={shlex.join(cmd)}")
    p = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        env=_child_env(),
        stdout=subprocess.DEVNULL,   # watchdog has its own logging file
        stderr=subprocess.DEVNULL,   # keep controller log clean
        close_fds=True,
        start_new_session=True,
    )
    return p


def _stop_proc(p: subprocess.Popen, logger, reason: str = "stop") -> None:
    """
    Try graceful stop, then SIGKILL if needed.
    """
    try:
        if p.poll() is None:
            logger.info(f"Stopping watchdog PID={p.pid} ({reason})")
            # SIGTERM child session
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except Exception:
                p.terminate()
            for _ in range(20):
                if p.poll() is not None:
                    break
                time.sleep(0.1)
        if p.poll() is None:
            logger.warning(f"Watchdog PID={p.pid} did not stop; killing")
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except Exception:
                p.kill()
    except Exception as e:
        logger.warning(f"While stopping PID={getattr(p, 'pid', '?')}: {e}")


# --------------------------------------------------------------------------------------
# Controller: process reconciliation + pipeline
# --------------------------------------------------------------------------------------
class Controller:
    def __init__(self, logger, settings: Dict):
        self.logger = logger
        self.settings = settings
        self.paths = resolve_all_paths(self.settings)

        # desired_targets: ip -> {ip, description, source_ip, paused}
        self.desired_targets: Dict[str, Dict] = {}

        # running watchdogs: ip -> Popen
        self.watchdogs: Dict[str, subprocess.Popen] = {}

        # policy
        pol = _read_policy(settings, logger)
        self.loop_seconds = pol["loop_seconds"]
        self.pipeline_every_seconds = pol["pipeline_every_seconds"]
        self.rerun_on_change = pol["rerun_on_change"]

        # mtimes to detect changes
        self._last_targets_mtime = _safe_mtime(CONFIG_FILE)
        self._last_settings_mtime = _safe_mtime(SETTINGS_FILE)

        # pipeline schedule
        self._last_pipeline_ts = 0.0

        # pipeline scripts (ordered)
        self.pipeline_scripts = [
            GRAPH_GENERATOR_SCRIPT,
            TS_EXPORTER_SCRIPT,
            HTML_GENERATOR_SCRIPT,
            INDEX_GENERATOR_SCRIPT,
        ]

    # ---------- pipeline ----------
    def _run_one(self, script_path: str) -> bool:
        name = os.path.basename(script_path)
        log_path = os.path.join(LOG_DIR, f"pipeline_{name}.log")
        with open(log_path, "a", encoding="utf-8") as lf:
            header = f"\n=== {time.strftime('%Y-%m-%dT%H:%M:%S')} | START {name} ===\n"
            lf.write(header)
            lf.flush()
            cmd = ["/usr/bin/python3", script_path, "--settings", SETTINGS_FILE]
            self.logger.info(f"[pipeline] Running {name} …  (log: {log_path})")
            r = subprocess.run(
                cmd,
                cwd=REPO_ROOT,
                env=_child_env(),
                stdout=lf,
                stderr=lf,
            )
            if r.returncode == 0:
                self.logger.info(f"[pipeline] {name} OK")
                return True
            else:
                self.logger.error(f"[pipeline] {name} failed with rc={r.returncode}")
                # Tail a bit of its log into controller.log for quick visibility
                try:
                    lf.flush()
                    with open(log_path, "r", encoding="utf-8") as rf:
                        tail = "".join(rf.readlines()[-20:])
                    for line in tail.rstrip().splitlines():
                        self.logger.error(f"[pipeline] {line}")
                    self.logger.error("[pipeline] --- end tail ---")
                except Exception:
                    pass
                return False

    def run_pipeline(self) -> bool:
        """
        Run the 4 stages in order. If a stage fails, stop and return False.
        """
        ok = True
        for sp in self.pipeline_scripts:
            if not self._run_one(sp):
                ok = False
                break
        return ok

    # ---------- watchdog reconciliation ----------
    def _spawn_needed(self):
        """
        Start watchdogs for any active desired target that doesn't have one yet.
        """
        for ip, t in self.desired_targets.items():
            if t.get("paused"):
                continue
            if ip in self.watchdogs:
                continue
            self.watchdogs[ip] = _start_watchdog(
                ip=ip, settings_file=SETTINGS_FILE, source_ip=t.get("source_ip"), logger=self.logger
            )

    def _stop_unwanted(self):
        """
        Stop watchdogs that are no longer desired (removed or paused).
        """
        for ip in list(self.watchdogs.keys()):
            t = self.desired_targets.get(ip)
            if (t is None) or t.get("paused", False):
                _stop_proc(self.watchdogs[ip], self.logger, reason="undesired")
                self.watchdogs.pop(ip, None)

    def _reap_and_restart(self):
        """
        If a watchdog died but is still desired, restart it.
        """
        for ip, p in list(self.watchdogs.items()):
            if p.poll() is not None:
                rc = p.returncode
                self.logger.warning(f"Watchdog for {ip} exited rc={rc}; restarting if still desired.")
                self.watchdogs.pop(ip, None)
                # still desired?
                t = self.desired_targets.get(ip)
                if t and not t.get("paused"):
                    self.watchdogs[ip] = _start_watchdog(
                        ip=ip, settings_file=SETTINGS_FILE, source_ip=t.get("source_ip"), logger=self.logger
                    )

    def reconcile_targets(self, targets: List[Dict]):
        """
        Main entrypoint to apply a new targets list.
        """
        self.desired_targets = {t["ip"]: t for t in targets if t.get("ip")}
        self._stop_unwanted()
        self._spawn_needed()

    # ---------- main loop steps ----------
    def _maybe_reload_settings(self):
        """
        Reload settings when the YAML changes; refresh log levels; adjust policy; optionally run pipeline.
        """
        curr = _safe_mtime(SETTINGS_FILE)
        if curr != self._last_settings_mtime:
            self.settings = load_settings(SETTINGS_FILE)
            # IMPORTANT: the utils signature accepts at most 2 positional args. Use keywords.
            refresh_logger_levels(logger=self.logger, settings=self.settings)
            self._last_settings_mtime = curr
            self.logger.info("Settings reloaded; 'controller' log level refreshed.")

            pol = _read_policy(self.settings, self.logger)
            self.loop_seconds = pol["loop_seconds"]
            self.pipeline_every_seconds = pol["pipeline_every_seconds"]
            self.rerun_on_change = pol["rerun_on_change"]

            if self.rerun_on_change:
                self.logger.info("Running pipeline due to settings change.")
                if self.run_pipeline():
                    self._last_pipeline_ts = time.time()

    def _maybe_reload_targets(self):
        """
        Reload targets when mtr_targets.yaml changes; reconcile watchdogs; optionally run pipeline.
        """
        curr = _safe_mtime(CONFIG_FILE)
        if curr != self._last_targets_mtime:
            targets = _load_targets(self.logger)
            self._last_targets_mtime = curr
            self.logger.info(f"Targets changed; reconciling {len(targets)} targets.")
            self.reconcile_targets(targets)
            if self.rerun_on_change:
                self.logger.info("Running pipeline due to targets change.")
                if self.run_pipeline():
                    self._last_pipeline_ts = time.time()

    def _maybe_run_scheduled_pipeline(self):
        now = time.time()
        if (now - self._last_pipeline_ts) >= max(5, self.pipeline_every_seconds):
            self.logger.debug("Time-based pipeline trigger.")
            if self.run_pipeline():
                self._last_pipeline_ts = now

    def tick(self):
        """
        One iteration of the controller loop.
        """
        self._maybe_reload_settings()
        self._maybe_reload_targets()
        self._reap_and_restart()
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

    # Initial targets & spawn
    targets = _load_targets(logger)
    logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
    ctl.reconcile_targets(targets)

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
            # Sleep between scans
            stop_evt.wait(timeout=max(1, ctl.loop_seconds))
    finally:
        # Stop all watchdogs
        logger.info("Stopping all watchdogs…")
        for ip, p in list(ctl.watchdogs.items()):
            _stop_proc(p, logger, reason="shutdown")
        logger.info("Controller stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
