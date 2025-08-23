#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/mtr_watchdog.py
=======================

Purpose
-------
Entrypoint "watchdog" for MTR_WEB. It:

1) Loads YAML settings.
2) Resolves/creates required directories that are safe to materialize.
3) Acquires a *single-writer lock* under the YAML traceroute directory so that
   only this process is allowed to write traceroute artifacts (*.trace.txt, *_hops.json, *_hops_stats.json).
4) Starts and supervises per-target monitoring workers (optional, see below).
   - This template tries to import a 'monitor_target' callable from known modules.
   - If you already have your own runner/controller, you can disable the spawning section.

Strict Traceroute Path
----------------------
- The traceroute directory is taken *only* from settings['paths']['traceroute'].
- If it is missing or does not exist, we exit fatally — this prevents split-directory writes.

Signals
-------
- SIGINT/SIGTERM trigger a graceful shutdown of child workers and lock release.

Notes
-----
- If you already start per-IP monitors elsewhere (e.g., a separate controller),
  set SPAWN_WORKERS = False below; the watchdog will only enforce the writer lock.
- Workers are started via multiprocessing; each receives (ip, settings) by default.

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
# Single-writer file lock (inlined to avoid extra dependency/module)
# -----------------------------------------------------------------------------

class SingleWriterLock:
    """
    File-based exclusive lock to guarantee one traceroute writer.
    Keep an instance alive for the watchdog lifetime (don't let it be GC'd).
    """
    def __init__(self, lockfile: str):
        self.lockfile = lockfile
        self.fd: Optional[int] = None

    def acquire(self):
        # Import here to keep top-level import list minimal
        import fcntl
        os.makedirs(os.path.dirname(self.lockfile), exist_ok=True)
        self.fd = os.open(self.lockfile, os.O_RDWR | os.O_CREAT, 0o644)
        # Non-blocking exclusive lock; raises BlockingIOError if already locked
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
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
# Targets loader (robust to common YAML shapes)
# -----------------------------------------------------------------------------

def _load_targets(settings: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    """
    Read targets from:
      1) settings['files']['targets'] if present, else
      2) 'mtr_targets.yaml' in the project root (cwd).

    Accepts either:
      - list of {ip: "...", description: "...", pause: bool}, or
      - dict mapping "ip" -> {description, pause, ...}

    Returns a list of normalized dicts: [{'ip': '1.1.1.1', 'description': str, 'pause': bool}, ...]
    """
    # Determine file path
    files = settings.get("files", {})
    path = files.get("targets") or "mtr_targets.yaml"
    if not os.path.isabs(path):
        # try relative to the settings file directory if available
        # settings dict may include a hint; if not, use cwd
        pass  # keep path as-is; cwd should be project root in systemd unit

    if not os.path.isfile(path):
        logger.warning(f"Targets file not found: {path} (watchdog will run without spawning workers).")
        return []

    # Parse YAML
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

    # Drop paused
    active = [t for t in targets if not t.get("pause")]
    return active


# -----------------------------------------------------------------------------
# Dynamic import of the monitor entrypoint
# -----------------------------------------------------------------------------

def _import_monitor(logger) -> Optional[Callable]:
    """
    Try a sequence of known locations to find `monitor_target`.
    Adjust the list if your project uses a different module path.
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
    Child process target. Re-import the entrypoint in the child (safe for fork/spawn),
    then call monitor_target(ip, settings=settings).
    """
    # Defer imports to child
    import importlib
    import logging

    logger = setup_logger(f"{ip}", settings=settings, logfile=None)  # per-IP console+file via settings

    mod_name, func_name = entrypoint_path.split(":")
    try:
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, func_name)
    except Exception as e:
        logger.error(f"[{ip}] Failed to import monitor entrypoint {entrypoint_path}: {e}")
        return

    try:
        # Try two common calling conventions
        try:
            fn(ip, settings=settings)
        except TypeError:
            fn(ip, settings)  # legacy positional
    except KeyboardInterrupt:
        logger.info(f"[{ip}] Monitor interrupted (KeyboardInterrupt).")
    except Exception as e:
        logger.exception(f"[{ip}] Monitor crashed: {e}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="MTR_WEB Watchdog")
    parser.add_argument(
        "--settings",
        default="mtr_script_settings.yaml",
        help="Path to YAML settings (default: mtr_script_settings.yaml)",
    )
    parser.add_argument(
        "--no-spawn",
        action="store_true",
        help="Do not spawn per-IP monitors; only acquire writer lock and stay alive.",
    )
    args = parser.parse_args(argv)

    # 1) Load settings & logger
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        print(f"[FATAL] Cannot load settings: {e}", file=sys.stderr)
        return 1

    logger = setup_logger("mtr_watchdog", settings=settings)

    # 2) Resolve paths (materialize safe ones)
    paths = resolve_all_paths(settings)
    tr_dir = paths.get("traceroute")
    if not tr_dir:
        logger.error("settings.paths.traceroute is missing or does not exist. Aborting.")
        return 1

    # 3) Acquire single-writer lock
    lock_path = os.path.join(tr_dir, ".writer.lock")
    writer_lock = SingleWriterLock(lock_path)
    try:
        writer_lock.acquire()
    except BlockingIOError:
        logger.error(f"Another process holds the traceroute writer lock: {lock_path}")
        return 2
    except Exception as e:
        logger.error(f"Failed to acquire traceroute writer lock: {e}")
        return 2

    logger.info(f"Acquired traceroute writer lock at {lock_path}")

    # Signal handling for graceful shutdown
    shutdown = {"flag": False}

    def _on_signal(signum, frame):
        logger.info(f"Received signal {signum}; shutting down.")
        shutdown["flag"] = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    # 4) Optionally spawn per-target workers
    SPAWN_WORKERS = (not args.no_spawn)

    procs: List[mp.Process] = []
    entrypoint = None

    if SPAWN_WORKERS:
        entrypoint = _import_monitor(logger)
        if entrypoint is None:
            logger.error("No monitor entrypoint available; not spawning workers.")
            SPAWN_WORKERS = False

    # Normalize entrypoint path string for child import (module:function)
    entry_path = None
    if SPAWN_WORKERS and entrypoint is not None:
        entry_path = f"{entrypoint.__module__}:{entrypoint.__name__}"

    if SPAWN_WORKERS and entry_path:
        targets = _load_targets(settings, logger)
        if not targets:
            logger.warning("No active targets found; watchdog will idle.")
        else:
            logger.info(f"Starting {len(targets)} monitor worker(s).")
            for t in targets:
                ip = t["ip"]
                p = mp.Process(target=_worker_wrapper, args=(ip, settings, entry_path), daemon=True)
                p.start()
                procs.append(p)
                logger.info(f"[{ip}] Worker PID {p.pid} started.")

    # 5) Supervision loop
    try:
        while not shutdown["flag"]:
            # Reap dead processes (if any). You can add automatic restart logic here if desired.
            alive = []
            for p in procs:
                if p.is_alive():
                    alive.append(p)
                else:
                    logger.warning(f"Worker PID {p.pid} exited with code {p.exitcode}.")
            procs = alive

            time.sleep(1.0)
    finally:
        # Terminate child workers
        if procs:
            logger.info("Terminating child workers...")
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

        # Release writer lock
        try:
            writer_lock.release()
        except Exception:
            pass
        logger.info("Writer lock released. Bye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
