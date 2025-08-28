#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/mtr_watchdog.py
=======================

Role
----
Watchdog/launcher for MTR monitors.

Why this file changes
---------------------
Previously, imports like `modules.monitor` could fail if the process was started
from a different working directory or without PYTHONPATH set. This file now
bootstraps `sys.path` to include the repository `scripts/` directory so that
`import modules.*` is reliable without external environment tweaks.

Operating modes
---------------
1) Per-target mode (controller launches many):
   $ mtr_watchdog.py --target 8.8.8.8 --settings mtr_script_settings.yaml [--entry modules.monitor:monitor_target]
   - DOES NOT take the writer lock by default (prevents restart storms).
   - Use --writer if you explicitly want THIS process to hold the traceroute writer lock.

2) Aggregate mode (single process spawns workers itself):
   $ mtr_watchdog.py --settings mtr_script_settings.yaml
   - Spawns a worker per active target from mtr_targets.yaml.
   - Takes the writer lock unless you pass --no-spawn without --writer.

3) Lock-holder only (no workers; for environments that launch workers elsewhere):
   $ mtr_watchdog.py --settings mtr_script_settings.yaml --no-spawn --writer

Writer lock
-----------
- The traceroute writer lock is a plain file lock at <paths.traceroute>/.writer.lock.
- Only the process passing --writer will attempt to acquire it.

Entrypoint discovery
--------------------
- The monitor callable is discovered automatically as `monitor_target` in common modules:
    modules.monitor, modules.mtr_monitor, modules.mtr_runner, modules.mtr, mtr_monitor
- You can force it with:  --entry 'module:function'

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
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))  # make 'modules' importable
from typing import Dict, Any, List, Optional, Callable

# -----------------------------------------------------------------------------
# Bootstrap sys.path so 'modules.*' is importable without PYTHONPATH
# -----------------------------------------------------------------------------
def _bootstrap_sys_path() -> str:
    """
    Ensure the repository's 'scripts/' directory (this file's parent) is on sys.path.

    Returns the path inserted (or already present) for logging/inspection.
    """
    scripts_dir = os.path.abspath(os.path.dirname(__file__))  # .../scripts
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    return scripts_dir

_SCRIPTS_DIR = _bootstrap_sys_path()

from modules.utils import load_settings, resolve_all_paths, setup_logger  # now safe to import

# -----------------------------------------------------------------------------
# Single-writer file lock
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
        try:
            os.ftruncate(self.fd, 0)
        except Exception:
            pass
        os.write(self.fd, f"locked by {os.getpid()}\n".encode("utf-8"))

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
# Targets loader
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
def _import_monitor(logger, forced: Optional[str] = None) -> Optional[Callable]:
    """
    Import the monitor entrypoint.

    If 'forced' is provided as 'module:function', import exactly that.
    Otherwise, try common candidates that expose monitor_target(ip, settings=...).

    On failure, emits detailed reasons per candidate (module error / missing attr).
    """
    def _try(path: str) -> Optional[Callable]:
        mod_name, func_name = path.split(":")
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            logger.debug(f"Import failed for {mod_name}: {e}")
            return None
        fn = getattr(mod, func_name, None)
        if not callable(fn):
            logger.debug(f"Module '{mod_name}' has no callable '{func_name}'")
            return None
        return fn

    if forced:
        fn = _try(forced)
        if fn:
            logger.info(f"Using monitor entrypoint (forced): {forced}")
            return fn
        logger.error(f"Entrypoint not callable or import failed: {forced}")
        return None

    candidates = [
        "modules.monitor:monitor_target",
        "modules.mtr_monitor:monitor_target",
        "modules.mtr_runner:monitor_target",
        "modules.mtr:monitor_target",
        "mtr_monitor:monitor_target",  # legacy top-level module in scripts/
    ]
    for path in candidates:
        fn = _try(path)
        if fn:
            logger.info(f"Using monitor entrypoint: {path}")
            return fn

    logger.error(
        "None of the candidate monitor entrypoints could be imported:\n  - " +
        "\n  - ".join(candidates) +
        f"\nCWD={os.getcwd()} SCRIPTS_DIR={_SCRIPTS_DIR}\nPYTHONPATH={':'.join(sys.path)}"
    )
    return None


# -----------------------------------------------------------------------------
# Worker process wrapper
# -----------------------------------------------------------------------------
def _worker_wrapper(ip: str, settings: Dict[str, Any], entrypoint_path: str):
    """
    Child process target. Re-imports the monitor entrypoint and calls it.
    """
    # Ensure child has the same sys.path bootstrap (important for 'spawn' start method)
    _bootstrap_sys_path()

    from modules.utils import setup_logger  # re-import after bootstrap
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
    parser.add_argument("--entry",
                        help="Explicit monitor entrypoint as 'module:function', "
                             "e.g. modules.monitor:monitor_target")
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
        if not tr_dir or not os.path.isdir(tr_dir):
            logger.error("settings.paths.traceroute missing or not a directory; cannot hold writer lock.")
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
       try:
          from modules.monitor import monitor_target  # this is the canonical entrypoint
       except Exception as e:
          print(f"[FATAL] Cannot import modules.monitor: {e}", file=sys.stderr)
          return 1
          
       try:
          monitor_target(args.target, settings=settings)  # clean signature
          return 0
       except KeyboardInterrupt:
          return 0
       except Exception as e:
          import traceback
          print("[FATAL] monitor_target crashed:\n" + traceback.format_exc(), file=sys.stderr)
          return 1

    # Aggregate mode: spawn workers unless --no-spawn
    if not args.no_spawn:
        entrypoint = _import_monitor(logger, args.entry)
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
                logger.info(f"[{ip}] Worker
