"""
modules/controller_utils.py
---------------------------
Contains WatchdogManager, responsible for:
- Starting exactly one mtr_watchdog.py child per *active* target
- Stopping children for removed/paused targets
- Restarting children that died or need new CLI args (e.g., source_ip changed)

This module is intentionally small and focused for easy testing.
"""

import os
import sys
import subprocess
from typing import Dict, List, Optional


class WatchdogManager:
    """
    Manages the set of child processes (one per target).
    Each child is a separate 'mtr_watchdog.py' process with:
        --target <ip> --settings <settings.yaml> [--source <source_ip>]

    We keep a dict: ip -> {"proc": Popen, "source_ip": Optional[str]}
    """
    def __init__(self, repo_root: str, monitor_script: str, settings_file: str, logger):
        self.repo_root      = repo_root
        self.monitor_script = monitor_script
        self.settings_file  = settings_file
        self.logger         = logger
        self.python         = sys.executable or "/usr/bin/python3"
        self._procs: Dict[str, Dict] = {}

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
                stdout=subprocess.DEVNULL,  # all children write to their own log files
                stderr=subprocess.DEVNULL
            )
            self.logger.info(f"Started watchdog for {ip} (PID {p.pid}) args={args}")
            return p
        except Exception as e:
            self.logger.error(f"Failed to start watchdog for {ip}: {e}")
            return None

    def _terminate(self, ip: str):
        """Stop one watchdog process cleanly (TERM → wait → KILL)."""
        info = self._procs.get(ip)
        if not info:
            return
        proc: subprocess.Popen = info.get("proc")
        if proc and (proc.poll() is None):
            try:
                self.logger.info(f"Stopping watchdog for {ip} (PID {proc.pid})")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Watchdog for {ip} did not exit; killing.")
                    proc.kill()
            except Exception as e:
                self.logger.error(f"Error while stopping watchdog for {ip}: {e}")
        self._procs.pop(ip, None)

    # ---------- public API ----------

    def reconcile(self, desired_targets: List[Dict]):
        """
        Ensure the running set of watchdogs matches the desired target set.

        Behavior:
          - Start watchdog for every desired, non‑paused target that isn't running
          - Stop watchdog for every target removed or marked paused
          - Restart watchdog if its source_ip changed
          - Restart dead watchdogs
        """
        desired_by_ip = {t["ip"]: t for t in desired_targets}

        # Stop no‑longer‑desired or paused
        for ip in list(self._procs.keys()):
            want = desired_by_ip.get(ip)
            if (want is None) or want.get("paused", False):
                self._terminate(ip)

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
                self._terminate(ip)
                p = self._spawn(ip, src)
                if p:
                    self._procs[ip] = {"proc": p, "source_ip": src}

    def reap_and_restart(self, desired_targets: List[Dict]):
        """
        If any child exited, restart it if the target is still desired and not paused.
        """
        desired_by_ip = {t["ip"]: t for t in desired_targets}
        for ip, info in list(self._procs.items()):
            proc: subprocess.Popen = info.get("proc")
            if proc and (proc.poll() is not None):
                rc = proc.returncode
                self.logger.warning(f"Watchdog for {ip} exited rc={rc}; restarting if still desired.")
                self._terminate(ip)
                want = desired_by_ip.get(ip)
                if want and not want.get("paused", False):
                    src = want.get("source_ip")
                    p = self._spawn(ip, src)
                    if p:
                        self._procs[ip] = {"proc": p, "source_ip": src}

    def stop_all(self):
        """Stop every running watchdog."""
        for ip in list(self._procs.keys()):
            self._terminate(ip)
