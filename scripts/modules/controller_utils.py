"""
modules/controller_utils.py
---------------------------
WatchdogManager: start/stop/restart one mtr_watchdog.py per active target.
"""

import os
import sys
import subprocess
from typing import Dict, List, Optional


class WatchdogManager:
    def __init__(self, repo_root: str, monitor_script: str, settings_file: str, logger):
        # Absolute paths to avoid cwd confusion
        self.repo_root     = repo_root
        self.monitor_script = monitor_script
        self.settings_file  = settings_file
        self.logger         = logger
        self.python         = sys.executable or "/usr/bin/python3"
        # ip -> {"proc": Popen, "source_ip": str|None}
        self._procs: Dict[str, Dict] = {}

    # ---------- lifecycle helpers ----------

    def _start(self, ip: str, source_ip: Optional[str]) -> Optional[subprocess.Popen]:
        """Start one mtr_watchdog.py for a target."""
        args = [self.python, self.monitor_script, "--target", ip, "--settings", self.settings_file]
        if source_ip:
            args += ["--source", str(source_ip)]
        try:
            p = subprocess.Popen(
                args,
                cwd=self.repo_root,
                stdout=subprocess.DEVNULL,  # child logs to its own files
                stderr=subprocess.DEVNULL
            )
            self.logger.info(f"Started watchdog for {ip} (PID {p.pid}) args={args}")
            return p
        except Exception as e:
            self.logger.error(f"Failed to start watchdog for {ip}: {e}")
            return None

    def _stop(self, ip: str):
        """Terminate one watchdog (TERM → wait → KILL)."""
        info = self._procs.get(ip)
        if not info:
            return
        proc: subprocess.Popen = info.get("proc")
        if proc and proc.poll() is None:
            try:
                self.logger.info(f"Stopping watchdog for {ip} (PID {proc.pid})")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Watchdog for {ip} didn’t exit; killing.")
                    proc.kill()
            except Exception as e:
                self.logger.error(f"Error while stopping watchdog for {ip}: {e}")
        self._procs.pop(ip, None)

    # ---------- public API ----------

    def reconcile(self, targets: List[Dict]):
        """
        Compare desired targets with current processes:
          - start for active targets
          - stop for removed/paused targets
          - restart when source_ip changes or proc died
        """
        desired = {t["ip"]: t for t in targets}

        # stop removed / paused
        for ip in list(self._procs.keys()):
            want = desired.get(ip)
            if (want is None) or want.get("paused", False):
                self._stop(ip)

        # start or adjust
        for ip, t in desired.items():
            if t.get("paused", False):
                continue
            src = t.get("source_ip")
            info = self._procs.get(ip)
            if info is None:
                p = self._start(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}
                continue

            proc: subprocess.Popen = info.get("proc")
            dead = (proc is None) or (proc.poll() is not None)
            old_src = info.get("source_ip")
            if dead:
                self.logger.warning(f"Watchdog for {ip} not running; restarting.")
                p = self._start(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}
            elif old_src != src:
                self.logger.info(f"{ip}: source_ip changed {old_src} → {src}; restarting.")
                self._stop(ip)
                p = self._start(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}

    def reap_and_restart(self, desired_targets: List[Dict]):
        """
        If any watchdog exited, restart it if still desired and not paused.
        """
        desired = {t["ip"]: t for t in desired_targets}
        for ip, info in list(self._procs.items()):
            proc: subprocess.Popen = info.get("proc")
            if proc and (proc.poll() is not None):
                rc = proc.returncode
                self.logger.warning(f"Watchdog for {ip} exited rc={rc}; restarting if still desired.")
                self._stop(ip)
                want = desired.get(ip)
                if want and not want.get("paused", False):
                    p = self._start(ip, want.get("source_ip"))
                    if p:
                        self._procs[ip] = {"proc": p, "source_ip": want.get("source_ip")}

    def stop_all(self):
        """Stop every running watchdog."""
        for ip in list(self._procs.keys()):
            self._stop(ip)
