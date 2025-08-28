#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mtr_watchdog.py
================

Clean watchdog/launcher for per-target MTR monitors.

Goals
-----
- **No shims, no guessing**: we always call the canonical entrypoint
  `modules.monitor:monitor_target`.
- **Self-contained imports**: adds the repository `scripts/` folder to
  `sys.path` so `modules.*` can be imported regardless of CWD or service env.
- **YAML-first**: all paths and knobs come from `mtr_script_settings.yaml`.
- **Two modes**:
  1) Per-target (controller launches many): `--target <ip>`
     Runs one worker in the foreground; exits with the worker’s code.
  2) Aggregate (this process spawns workers for all active targets):
     No `--target` → loads targets file and spawns one process per target.

Options
-------
--settings  : Path to mtr_script_settings.yaml (default: mtr_script_settings.yaml)
--target    : Run exactly one monitor for the given IP/host in the foreground
--writer    : Acquire a single-writer file lock on <paths.traceroute>/.writer.lock
--no-spawn  : In aggregate mode, do not spawn workers (hold only the writer lock)

Signals
-------
SIGINT/SIGTERM: cleanly stop spawned children and release the writer lock.

Logging
-------
Uses the shared logger from modules.utils.setup_logger. Child workers get a
per-target logger named after the IP (no file log unless your utils enable it).

Documentation
-------------
This script is suitable for users with basic Python knowledge. The only thing
you need to adjust is the path to the YAML settings file via `--settings`;
all other paths are resolved from that file. No environment variables are
required and you do not need to edit PYTHONPATH.
"""

from __future__ import annotations

import os
import sys
import signal
import argparse
import multiprocessing as mp
from typing import Dict, Any, List, Optional

# -----------------------------------------------------------------------------
# Make 'modules.*' importable regardless of CWD or environment
# -----------------------------------------------------------------------------
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

# Now it’s safe to import our project modules
from modules.utils import load_settings, resolve_all_paths, setup_logger  # noqa: E402


# -----------------------------------------------------------------------------
# Single writer file lock (for traceroute artifacts)
# -----------------------------------------------------------------------------
class SingleWriterLock:
    """
    Simple file-based exclusive lock that ensures a single process is considered
    the "writer" for traceroute artifacts. Only acquired when --writer is set.
    """
    def __init__(self, lockfile: str):
        self.lockfile = lockfile
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        import fcntl
        os.makedirs(os.path.dirname(self.lockfile), exist_ok=True)
        self._fd = os.open(self.lockfile, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # raises if locked elsewhere
        try:
            os.ftruncate(self._fd, 0)
        except Exception:
            pass
        os.write(self._fd, f"locked by PID {os.getpid()}\n".encode("utf-8"))

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            import fcntl
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            try:
                os.close(self._fd)
            except Exception:
                pass
            self._fd = None


# -----------------------------------------------------------------------------
# Targets loader (YAML)
# -----------------------------------------------------------------------------
def load_targets_from_yaml(settings: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    """
    Loads targets from either:
      settings['files']['targets']  (if provided)
      or repository default 'mtr_targets.yaml' in the repo root.

    Accepts:
      - list of dicts: [{'ip': '8.8.8.8', 'description': '...', 'pause': false}, ...]
      - mapping: { '8.8.8.8': {'description': '...', 'pause': false}, ... }

    Returns active (non-paused) targets as:
      [{'ip': '8.8.8.8', 'description': '...'}, ...]
    """
    import yaml

    # Prefer explicit path in settings
    path = (settings.get("files") or {}).get("targets")
    if not path:
        # Default to repo root / mtr_targets.yaml
        repo_root = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
        path = os.path.join(repo_root, "mtr_targets.yaml")

    if not os.path.isfile(path):
        logger.warning(f"Targets file not found: {path} (no workers will be spawned).")
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Failed to read targets file {path}: {e}")
        return []

    logger.info(f"Using targets file: {path}")

    targets: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            ip = str(row.get("ip") or "").strip()
            if not ip:
                continue
            if row.get("pause") or row.get("paused"):
                continue
            targets.append({"ip": ip, "description": str(row.get("description") or "")})
    elif isinstance(data, dict):
        for ip, row in data.items():
            ip = str(ip or "").strip()
            if not ip:
                continue
            row = row or {}
            if row.get("pause") or row.get("paused"):
                continue
            targets.append({"ip": ip, "description": str(row.get("description") or "")})

    logger.info(f"Loaded {len(targets)} active target(s) from {os.path.basename(path)}")
    return targets


# -----------------------------------------------------------------------------
# Worker wrapper
# -----------------------------------------------------------------------------
def worker_run(ip: str, settings: Dict[str, Any]) -> None:
    """
    Child process entrypoint. Re-applies path bootstrap and runs the monitor.
    """
    # Ensure child process also has 'scripts/' on sys.path (relevant for spawn/forkserver)
    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)

    from modules.utils import setup_logger  # re-import after potential sys.path mutation
    from modules.monitor import monitor_target  # canonical entrypoint

    logger = setup_logger(f"{ip}", settings=settings, logfile=None)
    try:
        monitor_target(ip, settings=settings)
    except KeyboardInterrupt:
        logger.info(f"[{ip}] Monitor interrupted.")
    except Exception as e:
        logger.exception(f"[{ip}] Monitor crashed: {e}")


# -----------------------------------------------------------------------------
# Signal handling for aggregate mode
# -----------------------------------------------------------------------------
_TERMINATE = False

def _signal_handler(signum, frame):
    global _TERMINATE
    _TERMINATE = True


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Watchdog/launcher for MTR monitors")
    parser.add_argument("--settings", default="mtr_script_settings.yaml",
                        help="Path to settings YAML (default: mtr_script_settings.yaml)")
    parser.add_argument("--target",
                        help="Run a single monitor in the foreground for this IP/host.")
    parser.add_argument("--writer", action="store_true",
                        help="Acquire writer lock on <paths.traceroute>/.writer.lock in this process.")
    parser.add_argument("--no-spawn", action="store_true",
                        help="Aggregate mode only: do not spawn workers (hold only the writer lock).")
    args = parser.parse_args(argv)

    # Load settings + logger
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        print(f"[FATAL] Cannot load settings: {e}", file=sys.stderr)
        return 1

    logger = setup_logger("mtr_watchdog", settings=settings)
    paths = resolve_all_paths(settings)

    # Acquire writer lock if requested
    writer_lock = None
    if args.writer:
        tr_dir = paths.get("traceroute")
        if not tr_dir or not os.path.isdir(tr_dir):
            logger.error("settings.paths.traceroute missing or not a directory; cannot hold writer lock.")
            return 1
        lock_path = os.path.join(tr_dir, ".writer.lock")
        writer_lock = SingleWriterLock(lock_path)
        try:
            writer_lock.acquire()
            logger.info(f"Acquired traceroute writer lock at {lock_path}")
        except BlockingIOError:
            logger.error(f"Another process already holds the writer lock: {lock_path}")
            return 2
        except Exception as e:
            logger.error(f"Failed to acquire writer lock: {e}")
            return 2

    # -------- Per-target mode: run one monitor in the foreground --------
    if args.target:
        try:
            # Import here to get a clean, direct failure if the entrypoint is missing
            from modules.monitor import monitor_target
        except Exception as e:
            print(f"[FATAL] Cannot import modules.monitor: {e}", file=sys.stderr)
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return 1

        try:
            monitor_target(args.target, settings=settings)
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return 0
        except KeyboardInterrupt:
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return 0
        except Exception as e:
            import traceback
            print("[FATAL] monitor_target crashed:\n" + traceback.format_exc(), file=sys.stderr)
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return 1

    # -------- Aggregate mode: optionally spawn all targets --------
    if args.no_spawn:
        # Hold the writer lock only; useful if workers run elsewhere
        logger.info("Aggregate mode with --no-spawn: holding writer lock only.")
        try:
            signal.signal(signal.SIGINT, _signal_handler)
            signal.signal(signal.SIGTERM, _signal_handler)
        except Exception:
            pass
        while not _TERMINATE:
            signal.pause()  # sleep until a signal arrives
        if writer_lock:
            try:
                writer_lock.release()
            except Exception:
                pass
        return 0

    # Spawn one worker per active target and supervise them
    targets = load_targets_from_yaml(settings, logger)
    if not targets:
        logger.warning("No active targets found; nothing to spawn.")
        if writer_lock:
            try:
                writer_lock.release()
            except Exception:
                pass
        return 0

    # Install signal handlers to terminate children cleanly
    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        pass

    procs: List[mp.Process] = []
    for t in targets:
        ip = t["ip"]
        p = mp.Process(target=worker_run, args=(ip, settings), daemon=True)
        p.start()
        procs.append(p)
        logger.info(f"[{ip}] Worker started (PID={p.pid})")

    # Supervise until termination signal
    try:
        while not _TERMINATE:
            any_alive = False
            for p in procs:
                if p.is_alive():
                    any_alive = True
                else:
                    # Keep the supervisor simple: do not auto-restart here.
                    # Your controller (if any) can decide restart policy.
                    pass
            if not any_alive:
                break
            signal.pause()  # sleep until a signal (or use time.sleep(1))
    except KeyboardInterrupt:
        pass
    finally:
        # Terminate children
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            try:
                p.join(timeout=5)
            except Exception:
                pass
        if writer_lock:
            try:
                writer_lock.release()
            except Exception:
                pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
