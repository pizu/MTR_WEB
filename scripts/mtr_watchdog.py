#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/mtr_watchdog.py
=======================

Role
----
Watchdog/launcher for MTR monitors.

Two modes:
1) Per-target mode (controller launches many):
   $ mtr_watchdog.py --target 8.8.8.8 --settings mtr_script_settings.yaml
   - DOES NOT take the writer lock by default (prevents restart storms).
   - Use --writer if you explicitly want THIS instance to be the single writer.

2) Aggregate mode (single process spawns workers itself):
   $ mtr_watchdog.py --settings mtr_script_settings.yaml
   - Spawns a worker per active target from mtr_targets.yaml.
   - Takes the writer lock (single writer).

You can also use:
   $ mtr_watchdog.py --settings mtr_script_settings.yaml --no-spawn --writer
   - Single process that ONLY holds the writer lock (no workers).
   - Useful if your monitors are launched elsewhere but you still want one writer.

Writer lock
-----------
- The traceroute writer lock lives at <paths.traceroute>/.writer.lock.
- Only the process that passes --writer will attempt to acquire it.

Signals
-------
- SIGINT/SIGTERM trigger graceful shutdown.

"""

from __future__ import annotations

import os
import sys
import time
import signal
import argparse
import importlib
import multiprocessing as mp
from typing import Dict, Any, List, Optional, Callable

from modules.utils import load_settings, resolve_all_paths, setup_logger


# -----------------------------------------------------------------------------
# Single-writer file lock (local class)
# -----------------------------------------------------------------------------

class SingleWriterLock:
    """File-based exclusive lock to guarantee one traceroute writer."""
    def __init__(self, lockfile: str):
        self.lockfile = lockfile
        self.fd: Optional[int] = None

    def acquire(self):
        import fcntl
        os.makedirs(os.path.dirname(self.lockfile), exist_ok=True)
        self.fd = os.open(self.lockfile, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # raises if locked elsewhere
        os.write(self.fd, b"monitor-writer\n")

    def release(self):
        if self.fd is None:
            return
        try:
            import fcntl
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            try:
                os.close(self.fd)
            except Exception:
                pass
            self.fd = None


# -----------------------------------------------------------------------------
# Targets loader (robust)
# -----------------------------------------------------------------------------

def _load_targets(settings: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    """
    Load targets from settings['files']['targets'] or 'mtr_targets.yaml'.
    Accepts either list-of-dicts or mapping ip->dict. Drops paused targets.
    """
    files = settings.get("files", {})
    path = files.get("targets") or "mtr_targets.yaml"

    if not os.path.isfile(path):
        logger.warning(f"Targets file not found: {path} (no workers will be spawned).")
        return []

    import yaml
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    targets: List[Dict[str, Any]] = []

    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            ip = str(row.get("ip") or "").strip()
            if not ip:
                continue
            targets.append({
                "ip": ip,
                "description": str(row.get("description") or ""),
                "pause": bool(row.get("pause") or row.get("paused") or False),
            })
    elif isinstance(data, dict):
        for ip, row in data.items():
            if not ip:
                continue
            row = row or {}
            targets.append({
                "ip": str(ip).strip(),
                "description": str(row.get("description") or ""),
                "pause": bool(row.get("pause") or row.get("paused") or False),
            })

    return [t for t in targets if not t.get("pause")]


# -----------------------------------------------------------------------------
# Monitor entrypoint import
# -----------------------------------------------------------------------------

def _import_monitor(logger) -> Optional[Callable]:
    """
    Try a set of known locations for `monitor_target`.
    Adjust as needed for your codebase.
    """
    candidates = [
        "modules.monitor:monitor_target",
        "modules.mtr_monitor:monitor_target",
        "modules.mtr_runner:monitor_target",
        "modules.mtr:monitor_target",
        "mtr_monitor:monitor_target",  # legacy
    ]
    for path in candidates:
        mod_name, func_name = path.split(":")
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, func_name, None)
            if callable(fn):
                logger.info(f"Using monitor entrypoint: {path}")
                return fn
        except Exception as e:
            logger.debug(f"Import failed for {path}: {e}")
    logger.error(
        "None of the candidate monitor entrypoints could be imported:\n  - " +
        "\n  - ".join(candidates)
    )
    return None


# -----------------------------------------------------------------------------
# Worker process wrapper
# -----------------------------------------------------------------------------

def _worker_wrapper(ip: str, settings: Dict[str, Any], entrypoint_path: str):
    """
    Child process target. Re-imports the monitor entrypoint and calls it.
    """
    import importlib
    from modules.utils import setup_logger

    logger = setup_logger(f"{ip}", settings=settings, logfile=None)

    mod_name, func_name = entrypoint_path.split(":")
    try:
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, func_name)
    except Exception as e:
        logger.error(f"[{ip}] Failed to import monitor entrypoint {entrypoint_path}: {e}")
        return

    try:
        # Preferred signature
        try:
            fn(ip, settings=settings)
        except TypeError:
            fn(ip, settings)  # legacy positional
    except KeyboardInterrupt:
        logger.info(f"[{ip}] Monitor interrupted.")
    except Exception as e:
        logger.exception(f"[{ip}] Monitor crashed: {e}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="MTR_WEB Watchdog")
    parser.add_argument("--settings", default="mtr_script_settings.yaml",
                        help="Path to YAML settings (default: mtr_script_settings.yaml)")
    parser.add_argument("--target", help="Run a single per-target worker (IP or hostname).")
    parser.add_argument("--writer", action="store_true",
                        help="Attempt to acquire the traceroute writer lock in this process.")
    parser.add_argument("--no-spawn", action="store_true",
                        help="Do not spawn per-IP workers (aggregate mode only).")
    args = parser.parse_args(argv)

    # Load settings & logger
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        print(f"[FATAL] Cannot load settings: {e}", file=sys.stderr)
        return 1

    logger = setup_logger("mtr_watchdog", settings=settings)
    paths = resolve_all_paths(settings)
    tr_dir = paths.get("traceroute")

    # If we are the WRITER, the traceroute dir MUST exist.
    if args.writer:
        if not tr_dir:
            logger.error("settings.paths.traceroute is missing or not a directory; cannot hold writer lock.")
            return 1

    # Acquire writer lock ONLY if requested
    writer_lock = None
    if args.writer:
        lock_path = os.path.join(tr_dir, ".writer.lock")
        writer_lock = SingleWriterLock(lock_path)
        try:
            writer_lock.acquire()
            logger.info(f"Acquired traceroute writer lock at {lock_path}")
        except BlockingIOError:
            logger.error(f"Another process holds the traceroute writer lock: {lock_path}")
            return 2
        except Exception as e:
            logger.error(f"Failed to acquire traceroute writer lock: {e}")
            return 2

    # Per-target mode: controller runs many instances with --target
    if args.target:
        entrypoint = _import_monitor(logger)
        if not entrypoint:
            return 3
        entry_path = f"{entrypoint.__module__}:{entrypoint.__name__}"

        # Run worker in-foreground (no extra process), so controller gets the exit code
        _worker_wrapper(args.target, settings, entry_path)

        # Release lock if we were the writer
        if writer_lock:
            try:
                writer_lock.release()
            except Exception:
                pass
        return 0

    # Aggregate mode: spawn workers unless --no-spawn
    if not args.no_spawn:
        entrypoint = _import_monitor(logger)
        if not entrypoint:
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return 3
        entry_path = f"{entrypoint.__module__}:{entrypoint.__name__}"

        targets = _load_targets(settings, logger)
        if not targets:
            logger.warning("No active targets found; watchdog will idle.")
        else:
            logger.info(f"Starting {len(targets)} monitor worker(s).")
            procs: List[mp.Process] = []
            for t in targets:
                ip = t["ip"]
                p = mp.Process(target=_worker_wrapper, args=(ip, settings, entry_path), daemon=True)
                p.start()
                procs.append(p)
                logger.info(f"[{ip}] Worker PID {p.pid} started.")

            # Supervise workers until interrupted
            try:
                while True:
                    procs = [p for p in procs if p.is_alive()]
                    time.sleep(1.0)
            except KeyboardInterrupt:
                logger.info("Interrupt received; terminating workers...")
            finally:
                for p in procs:
                    try:
                        if p.is_alive():
                            p.terminate()
                    except Exception:
                        pass
                for p in procs:
                    try:
                        p.join(timeout=3.0)
                    except Exception:
                        pass

    # Release writer lock (if held)
    if writer_lock:
        try:
            writer_lock.release()
        except Exception:
            pass
        logger.info("Writer lock released.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
