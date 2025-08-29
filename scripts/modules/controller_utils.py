# modules/controller_utils.py
# ===========================
# Small, testable helpers used by controller.py
#
# CHANGES (PNG REMOVAL):
# - PipelineRunner no longer references or runs graph_generator.py
# - No component in this file can indirectly create PNGs anymore

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

try:
    from modules.utils import (
        refresh_logger_levels as _utils_refresh_logger_levels,
        resolve_targets_path as _utils_resolve_targets_path,
    )
except Exception:
    _utils_refresh_logger_levels = None
    _utils_resolve_targets_path = None


def safe_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


def child_env(scripts_dir: str) -> Dict[str, str]:
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    paths = [scripts_dir] + ([pp] if pp else [])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def load_targets(config_file: str, logger) -> List[Dict]:
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


@dataclass
class ControllerPolicy:
    loop_seconds: int = 2
    pipeline_every_seconds: int = 60
    rerun_on_change: bool = True

    @classmethod
    def from_settings(cls, settings: Dict, logger) -> "ControllerPolicy":
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


class ConfigWatcher:
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


class PipelineRunner:
    """
    Runs the reporting pipeline in order (PNG removed):
      timeseries_exporter.py → html_generator.py → index_generator.py

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

        # Absolute paths to child scripts (graph_generator removed)
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
        """Run all stages (without PNG graphs); stop on first failure."""
        for sp in (self.ts_script, self.html_script, self.index_script):
            if not self._run_one(sp):
                return False
        return True


class WatchdogManager:
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

    def _spawn(self, ip: str, source_ip: Optional[str]) -> Optional[subprocess.Popen]:
        args = [self.python, self.monitor_script, "--target", ip, "--settings", self.settings_file]
        if source_ip:
            args += ["--source", str(source_ip)]
        try:
            p = subprocess.Popen(
                args,
                cwd=self.repo_root,
                env=self._env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                start_new_session=True,
            )
            self.logger.info(f"Started watchdog for {ip} (PID {p.pid}) args={shlex.join(args)}")
            return p
        except Exception as e:
            self.logger.error(f"Failed to start watchdog for {ip}: {e}")
            return None

    def _terminate(self, ip: str, reason: str = "stop"):
        info = self._procs.get(ip)
        if not info:
            return
        proc: subprocess.Popen = info.get("proc")
        if proc and (proc.poll() is None):
            try:
                self.logger.info(f"Stopping watchdog for {ip} (PID {proc.pid}) ({reason})")
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

    def reconcile(self, desired_targets: List[Dict]):
        desired_by_ip = {t["ip"]: t for t in desired_targets}

        for ip in list(self._procs.keys()):
            want = desired_by_ip.get(ip)
            if (want is None) or want.get("paused", False):
                self._terminate(ip, reason="undesired")

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
        for ip in list(self._procs.keys()):
            self._terminate(ip, reason="shutdown")


def refresh_logging_from_settings(settings: Dict) -> None:
    if _utils_refresh_logger_levels:
        _utils_refresh_logger_levels(settings=settings)


def targets_path(settings: Optional[Dict] = None) -> str:
    if _utils_resolve_targets_path:
        return _utils_resolve_targets_path(settings)
    base = (settings or {}).get("_meta", {}).get("settings_dir") or os.getcwd()
    cand = os.path.join(base, "mtr_targets.yaml")
    return os.path.abspath(cand if os.path.isfile(cand) else "mtr_targets.yaml")
