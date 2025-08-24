"""
modules/controller_utils.py
===========================
Small, testable helpers used by controller.py:

- ControllerPolicy                    → parse controller.* settings with BC aliases
- safe_mtime(path)                    → robust getmtime
- child_env(scripts_dir)             → ensure PYTHONPATH includes scripts/
- load_targets(config_file, logger)   → normalize targets from mtr_targets.yaml
- ConfigWatcher                       → watches settings/targets YAML for changes
- PipelineRunner                      → runs the 4 pipeline stages with per-stage logs
- WatchdogManager                     → 1 watchdog process per active target
- refresh_logging_from_settings(...)  → proxy to modules.utils.refresh_logger_levels
- targets_path(settings)              → proxy to modules.utils.resolve_targets_path

This module lets controller.py focus on orchestration instead of implementation.
"""

from __future__ import annotations
import os
import sys
import time
import shlex
import signal
import yaml
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional

# NOTE: we only import the utils symbols that we want to expose as clean wrappers.
# We NEVER configure handlers here—controller.py should create the logger via utils.setup_logger.
try:
    from modules.utils import (
        refresh_logger_levels as _utils_refresh_logger_levels,
        resolve_targets_path as _utils_resolve_targets_path,
    )
except Exception:
    # Soft fallback: in unit tests or partial environments, these may be absent.
    _utils_refresh_logger_levels = None
    _utils_resolve_targets_path = None


# --------------------------------------------------------------------------------------
# Lightweight helpers
# --------------------------------------------------------------------------------------

def safe_mtime(path: str) -> float:
    """Return file modification time or 0.0 if missing/inaccessible."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


def child_env(scripts_dir: str) -> Dict[str, str]:
    """
    Return environment for child processes.
    Ensures PYTHONPATH contains scripts/ so module imports work under systemd and shell.
    """
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    paths = [scripts_dir] + ([pp] if pp else [])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def load_targets(config_file: str, logger) -> List[Dict]:
    """
    Parse mtr_targets.yaml into normalized items:

        { "ip": "8.8.8.8",
          "description": "Google DNS",
          "source_ip": "192.0.2.10" | None,
          "paused": false }

    Missing/blank 'ip' rows are ignored.
    """
    try:
        with open(config_file, "r", encoding="utf-8") as f:
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
        logger.error(f"Failed to read {config_file}: {e}")
        return []


# --------------------------------------------------------------------------------------
# Controller policy
# --------------------------------------------------------------------------------------

@dataclass
class ControllerPolicy:
    loop_seconds: int = 2
    pipeline_every_seconds: int = 60
    rerun_on_change: bool = True

    @classmethod
    def from_settings(cls, settings: Dict, logger) -> "ControllerPolicy":
        """
        Read controller.* with backward-compatible aliases:
          - controller.scan_interval_seconds           -> loop_seconds
          - controller.pipeline_run_every_seconds      -> pipeline_every_seconds
          - controller.pipeline_run_on_change          -> rerun_on_change
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
        return cls(loop_seconds=loop_seconds,
                   pipeline_every_seconds=pipeline_every_seconds,
                   rerun_on_change=rerun_on_change)


# --------------------------------------------------------------------------------------
# File change watcher
# --------------------------------------------------------------------------------------

class ConfigWatcher:
    """
    Watches two files (settings, targets) for mtime changes.
    """
    def __init__(self, settings_file: str, targets_file: str):
        self.settings_file = settings_file
        self.targets_file  = targets_file
        self._last_settings_mtime = safe_mtime(settings_file)
        self._last_targets_mtime  = safe_mtime(targets_file)

    def settings_changed(self) -> bool:
        curr = safe_mtime(self.settings_file)
        if curr != self._last_settings_mtime:
            self._last_settings_mtime = curr
            return True
        return False

    def targets_changed(self) -> bool:
        curr = safe_mtime(self.targets_file)
        if curr != self._last_targets_mtime:
            self._last_targets_mtime = curr
            return True
        return False


# --------------------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------------------

class PipelineRunner:
    """
    Runs the reporting pipeline in order:
      graph_generator.py → timeseries_exporter.py → html_generator.py → index_generator.py

    - Writes each stage’s stdout/stderr to logs/pipeline_<script>.log
    - Stops on first failure and returns False
    """
    def __init__(self, repo_root: str, scripts_dir: str, settings_file: str, log_dir: str, logger):
        self.repo_root     = repo_root
        self.scripts_dir   = scripts_dir
        self.settings_file = settings_file
        self.log_dir       = log_dir
        self.logger        = logger
        self.python        = sys.executable or "/usr/bin/python3"
        # Absolute paths to child scripts
        self.graph_script  = os.path.join(scripts_dir, "graph_generator.py")
        self.ts_script     = os.path.join(scripts_dir, "timeseries_exporter.py")
        self.html_script   = os.path.join(scripts_dir, "html_generator.py")
        self.index_script  = os.path.join(scripts_dir, "index_generator.py")
        self._env          = child_env(self.scripts_dir)

    def _run_one(self, script_path: str) -> bool:
        name = os.path.basename(script_path)
        os.makedirs(self.log_dir, exist_ok=True)
        log_path = os.path.join(self.log_dir, f"pipeline_{name}.log")
        with open(log_path, "a", encoding="utf-8") as lf:
            header = f"\n=== {time.strftime('%Y-%m-%dT%H:%M:%S')} | START {name} ===\n"
            lf.write(header)
            lf.flush()
            cmd = [self.python, script_path, "--settings", self.settings_file]
            self.logger.info(f"[pipeline] Running {name} …  (log: {log_path})")
            r = subprocess.run(
                cmd,
                cwd=self.repo_root,
                env=self._env,
                stdout=lf,
                stderr=lf,
            )
            if r.returncode == 0:
                self.logger.info(f"[pipeline] {name} OK")
                return True
            self.logger.error(f"[pipeline] {name} failed with rc={r.returncode}")
            # Tail a bit for quick visibility
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

    def run_all(self) -> bool:
        """Run all pipeline stages in order; stop on first failure."""
        for sp in (self.graph_script, self.ts_script, self.html_script, self.index_script):
            if not self._run_one(sp):
                return False
        return True


# --------------------------------------------------------------------------------------
# Watchdog Manager
# --------------------------------------------------------------------------------------

class WatchdogManager:
    """
    Manages mtr_watchdog.py child processes, exactly one per active target.

    Keeps: ip → {"proc": Popen, "source_ip": Optional[str]}
    """
    def __init__(self, repo_root: str, scripts_dir: str, monitor_script: str,
                 settings_file: str, logger):
        self.repo_root      = repo_root
        self.scripts_dir    = scripts_dir
        self.monitor_script = monitor_script
        self.settings_file  = settings_file
        self.logger         = logger
        self.python         = sys.executable or "/usr/bin/python3"
        self._procs: Dict[str, Dict] = {}
        self._env = child_env(self.scripts_dir)

    # ---------- internals ----------

    def _spawn(self, ip: str, source_ip: Optional[str]) -> Optional[subprocess.Popen]:
        """Start one watchdog process for the target."""
        args = [self.python, self.monitor_script, "--target", ip, "--settings", self.settings_file]
        if source_ip:
            args += ["--source", str(source_ip)]
        try:
            p = subprocess.Popen(
                args,
                cwd=self.repo_root,
                env=self._env,
                stdout=subprocess.DEVNULL,   # children log to their own files
                stderr=subprocess.DEVNULL,
                close_fds=True,
                start_new_session=True,      # allow kill of process group
            )
            self.logger.info(f"Started watchdog for {ip} (PID {p.pid}) args={shlex.join(args)}")
            return p
        except Exception as e:
            self.logger.error(f"Failed to start watchdog for {ip}: {e}")
            return None

    def _terminate(self, ip: str, reason: str = "stop"):
        """Stop one watchdog process cleanly (TERM → wait → KILL)."""
        info = self._procs.get(ip)
        if not info:
            return
        proc: subprocess.Popen = info.get("proc")
        if proc and (proc.poll() is None):
            try:
                self.logger.info(f"Stopping watchdog for {ip} (PID {proc.pid}) ({reason})")
                # Try SIGTERM on the process group; fall back to terminate()
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except Exception:
                    proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Watchdog for {ip} did not exit; killing.")
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except Exception:
                        proc.kill()
            except Exception as e:
                self.logger.error(f"Error while stopping watchdog for {ip}: {e}")
        self._procs.pop(ip, None)

    # ---------- public API ----------

    def reconcile(self, desired_targets: List[Dict]):
        """
        Ensure running watchdogs match desired targets:

          - Start watchdog for each desired, non‑paused target not running
          - Stop watchdog for removed/paused targets
          - Restart watchdog if its source_ip changed
          - Restart dead watchdogs
        """
        desired_by_ip = {t["ip"]: t for t in desired_targets}

        # Stop no‑longer‑desired or paused
        for ip in list(self._procs.keys()):
            want = desired_by_ip.get(ip)
            if (want is None) or want.get("paused", False):
                self._terminate(ip, reason="undesired")

        # Start / adjust desired
        for ip, t in desired_by_ip.items():
            if t.get("paused", False):
                continue
            src = t.get("source_ip")
            info = self._procs.get(ip)

            if info is None:
                p = self._spawn(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}
                continue

            proc: subprocess.Popen = info.get("proc")
            dead = (proc is None) or (proc.poll() is not None)
            old_src = info.get("source_ip")

            if dead:
                self.logger.warning(f"Watchdog for {ip} not running; restarting.")
                p = self._spawn(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}

            elif old_src != src:
                self.logger.info(f"{ip}: source_ip changed {old_src} → {src}; restarting.")
                self._terminate(ip, reason="source_ip change")
                p = self._spawn(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}

    def reap_and_restart(self, desired_targets: List[Dict]):
        """If any child exited, restart if still desired and not paused."""
        desired_by_ip = {t["ip"]: t for t in desired_targets}
        for ip, info in list(self._procs.items()):
            proc: subprocess.Popen = info.get("proc")
            if proc and (proc.poll() is not None):
                rc = proc.returncode
                self.logger.warning(f"Watchdog for {ip} exited rc={rc}; restarting if still desired.")
                self._terminate(ip, reason="reap")
                want = desired_by_ip.get(ip)
                if want and not want.get("paused", False):
                    src = want.get("source_ip")
                    p = self._spawn(ip, src)
                    if p:
                        self._procs[ip] = {"proc": p, "source_ip": src}

    def stop_all(self):
        """Stop every running watchdog."""
        for ip in list(self._procs.keys()):
            self._terminate(ip, reason="shutdown")


# --------------------------------------------------------------------------------------
# Logging + path convenience wrappers (delegating to modules.utils)
# --------------------------------------------------------------------------------------

def refresh_logging_from_settings(settings: Dict) -> None:
    """
    Centralized, utils-backed way to refresh logger levels across the app.
    This simply delegates to modules.utils.refresh_logger_levels(settings).

    Keep controller.py clean by calling:
        controller_utils.refresh_logging_from_settings(settings)
    """
    if _utils_refresh_logger_levels is None:
        return
    _utils_refresh_logger_levels(settings=settings)


def targets_path(settings: Optional[Dict] = None) -> str:
    """
    Wrapper for modules.utils.resolve_targets_path(settings), exposed here
    for callers that prefer to stay inside controller_utils.
    """
    if _utils_resolve_targets_path is None:
        # Fallback if utils isn’t available for some reason:
        base = (settings or {}).get("_meta", {}).get("settings_dir") or os.getcwd()
        cand = os.path.join(base, "mtr_targets.yaml")
        return os.path.abspath(cand if os.path.isfile(cand) else "mtr_targets.yaml")
    return _utils_resolve_targets_path(settings)
