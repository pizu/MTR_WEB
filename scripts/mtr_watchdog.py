#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/mtr_watchdog.py
=======================

Role
----
Watchdog/launcher for MTR monitors.

Operating modes
---------------
1) Per-target mode (controller launches many):
   $ mtr_watchdog.py --target 8.8.8.8 --settings mtr_script_settings.yaml [--entry modules.mtr_runner:monitor_target]
   - Does NOT take the writer lock by default (prevents restart storms).
   - Use --writer if you explicitly want THIS process to be the single traceroute writer.

2) Aggregate mode (one process spawns workers for all targets):
   $ mtr_watchdog.py --settings mtr_script_settings.yaml
   - If --no-spawn is not provided, spawns child processes per IP.

Exit codes (contract with controller)
------------------------------------
RC_OK              = 0  (clean finish)
RC_RETRYABLE_ERR   = 1  (transient error; safe to restart with backoff)
RC_FATAL_ERR       = 2  (configuration/packaging error; DO NOT restart)
RC_NOT_DESIRED     = 3  (paused/disabled/not in manifest; DO NOT restart)

Rationale: Previously, entrypoint import failure returned rc=3 which looked like
"not desired" and caused infinite restart loops. We now return rc=2 to stop loops
and point clearly to misconfiguration.
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import queue
import signal
import random
import atexit
import importlib
import argparse
import traceback
import multiprocessing as mp
from typing import Dict, Any, List, Optional, Callable

from modules.utils import (
    load_settings,
    resolve_all_paths,
    setup_logger,
    resolve_targets_path,  # NEW: unified resolver (supports top-level targets path)
)

# -----------------------------------------------------------------------------
# Exit codes (clear, controller-friendly semantics)
RC_OK = 0                 # Clean finish
RC_RETRYABLE_ERR = 1      # Transient failure -> controller may restart with backoff
RC_FATAL_ERR = 2          # Unrecoverable/config error -> controller should NOT restart
RC_NOT_DESIRED = 3        # Paused/disabled/not-in-manifest -> controller MUST NOT restart
# -----------------------------------------------------------------------------


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
# Targets
# -----------------------------------------------------------------------------

def _load_targets(settings: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    """
    Load targets from (in order):
      - settings['files']['targets']
      - settings['targets'] when it is a **string file path**
      - 'mtr_targets.yaml' next to the settings file
      - 'mtr_targets.yaml' in current working directory

    Accepts either list-of-dicts or mapping ip->dict. Drops paused targets.
    """
    path = resolve_targets_path(settings)

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
        # Also support dict form: { "1.1.1.1": {description: "...", pause: false}, ... }
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
    """
    if forced:
        try:
            mod_name, func_name = forced.split(":")
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, func_name, None)
            if callable(fn):
                logger.info(f"Using forced entrypoint: {forced}")
                return fn
            logger.error(f"Forced entrypoint not callable: {forced}")
            return None
        except Exception as e:
            logger.error(f"Failed to import forced entrypoint {forced}: {e}")
            return None

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
                logger.info(f"Using entrypoint: {path}")
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

def _worker_wrapper(target_ip: str, entrypoint: Callable, settings: Dict[str, Any]) -> int:
    """
    Child process wrapper that calls the actual monitor entrypoint.

    Expected entrypoint signature:
        monitor_target(ip: str, settings: dict) -> int
    where the returned int is traditionally 0 on success.

    This wrapper adds a safety net to ensure non-zero returns propagate up.
    """
    try:
        rc = int(entrypoint(target_ip, settings=settings))
    except KeyboardInterrupt:
        rc = 0
    except Exception:
        traceback.print_exc()
        rc = 1
    return rc


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
                             "e.g. modules.mtr_runner:monitor_target")
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
        return RC_FATAL_ERR

    logger = setup_logger("mtr_watchdog", settings=settings)
    paths = resolve_all_paths(settings)
    tr_dir = paths.get("traceroute")
    locks_dir = paths.get("locks_dir") or os.path.join(paths["data_dir"], ".locks")
    os.makedirs(locks_dir, exist_ok=True)

    # Acquire writer lock if requested
    writer_lock: Optional[SingleWriterLock] = None
    lock_path = os.path.join(locks_dir, "traceroute_writer.lock")
    if args.writer:
        writer_lock = SingleWriterLock(lock_path)
        try:
            writer_lock.acquire()
            logger.info(f"Acquired traceroute writer lock at {lock_path}")
        except BlockingIOError:
            logger.warning("Writer lock already held by another process; continuing without it.")
            writer_lock = None

    # --- PER-TARGET MODE ------------------------------------------------------
    if args.target:
        entrypoint = _import_monitor(logger, args.entry)
        if not entrypoint:
            # This is a configuration/packaging error -> FATAL (no restarts)
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return RC_FATAL_ERR

        logger.info(f"[{args.target}] starting monitor worker (per-target mode)")
        rc = _worker_wrapper(args.target, entrypoint, settings)

        # Release writer lock (if any)
        if writer_lock:
            try:
                writer_lock.release()
            except Exception:
                pass
            logger.info("Writer lock released.")

        return RC_OK if rc == 0 else RC_RETRYABLE_ERR

    # --- AGGREGATE MODE -------------------------------------------------------
    # In aggregate mode, this process spawns a child per target (unless --no-spawn).
    targets = _load_targets(settings, logger)
    logger.info(f"Discovered {len(targets)} targets.")

    if not targets:
        logger.warning("No targets to monitor; exiting NOT_DESIRED (rc=3).")
        if writer_lock:
            try:
                writer_lock.release()
            except Exception:
                pass
        return RC_NOT_DESIRED

    if not args.no_spawn:
        entrypoint = _import_monitor(logger, args.entry)
        if not entrypoint:
            if writer_lock:
                try:
                    writer_lock.release()
                except Exception:
                    pass
            return RC_FATAL_ERR

        procs: List[mp.Process] = []
        try:
            for t in targets:
                ip = t["ip"]
                p = mp.Process(target=_worker_wrapper, args=(ip, entrypoint, settings), daemon=True)
                p.start()
                procs.append(p)
                logger.info(f"[{ip}] worker pid={p.pid} started")

            # Wait for children
            for p in procs:
                try:
                    p.join()
                except KeyboardInterrupt:
                    break
                except Exception:
                    traceback.print_exc()
        finally:
            # Best-effort terminate any stragglers
            for p in procs:
                if p.is_alive():
                    try:
                        p.terminate()
                    except Exception:
                        pass
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

    return RC_OK


if __name__ == "__main__":
    raise SystemExit(main())
