#!/usr/bin/env python3
"""
mtr_watchdog.py
---------------
Per-target launcher invoked by controller.py.

Responsibilities:
  1) Parse CLI args (--settings, --target, --source).
  2) Load YAML settings.
  3) Initialize logging with a logger named 'mtr_watchdog' (+ per-target extra file).
  4) Import and call the actual monitor loop: monitor_target(ip, source_ip, settings, logger)
     - Prefer modules.monitor.monitor_target
     - Fallback to legacy scripts/mtr_monitor.monitor_target

Exit codes:
  0 = clean stop (Ctrl+C or normal return from monitor)
  1 = bad arguments / settings / initialization failure
  2 = monitor crashed with an unexpected exception

NOTE:
- Ensure 'mtr_watchdog' exists in mtr_script_settings.yaml under logging_levels, e.g.:
    logging_levels:
      mtr_watchdog: ERROR
"""

import os
import sys
import argparse
import logging
from typing import Optional

# --- Make sure 'modules' is importable even when launched by systemd from repo root ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# Shared helpers
try:
    from modules.utils import load_settings, setup_logger, refresh_logger_levels
except Exception as e:
    print(f"[FATAL] Cannot import modules.utils: {e}", file=sys.stderr)
    sys.exit(1)


def _import_monitor_target():
    """
    Try modern modular path first, then fallback to legacy layout.
    Returns: (callable monitor_target, origin_string)
    Raises: ImportError if neither is available.
    """
    # A) modules.monitor.monitor_target
    try:
        from modules.monitor import monitor_target  # type: ignore
        return monitor_target, "modules.monitor"
    except Exception:
        pass

    # B) legacy scripts/mtr_monitor.py
    try:
        if SCRIPTS_DIR not in sys.path:
            sys.path.insert(0, SCRIPTS_DIR)
        from mtr_monitor import monitor_target  # type: ignore
        return monitor_target, "mtr_monitor"
    except Exception as e:
        raise ImportError(
            "Could not import monitor_target from modules.monitor or mtr_monitor. "
            f"Original error: {e}"
        )


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments.
    --settings defaults to repo_root/mtr_script_settings.yaml (works when run from scripts/).
    """
    default_settings = os.path.join(os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir)), "mtr_script_settings.yaml")

    parser = argparse.ArgumentParser(description="Launch per-target MTR monitor loop.")
    parser.add_argument("--settings", default=default_settings,
                        help=f"Path to YAML settings (default: {default_settings})")
    parser.add_argument("--target", required=True,
                        help="Destination host/IP to monitor (e.g., 8.8.8.8)")
    parser.add_argument("--source", default=None,
                        help="Optional source IP to bind (passed to mtr)")
    return parser.parse_args()


def main() -> int:
    # 1) Arguments
    args = parse_args()

    # 2) Settings
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        print(f"[FATAL] Failed to load settings '{args.settings}': {e}", file=sys.stderr)
        return 1

    # 3) Logging — create the logger FIRST, then optionally refresh later
    try:
        log_dir = settings.get("log_directory", "/tmp")
        logger = setup_logger(
            "mtr_watchdog",
            log_dir,
            "mtr_watchdog.log",
            settings=settings,
            # Per-target extra file helps when tailing specific targets (e.g., 8.8.8.8.log)
            extra_file=f"{args.target}.log",
        )
        # If you later add hot-reload of settings in this file, call:
        # refresh_logger_levels(logger, "mtr_watchdog", settings)
    except Exception as e:
        print(f"[FATAL] Failed to initialize logging: {e}", file=sys.stderr)
        return 1

    logger.info(f"[{args.target}] Watchdog starting (settings='{args.settings}', source='{args.source}')")

    # 4) Import monitor_target implementation
    try:
        monitor_target, origin = _import_monitor_target()
        logger.debug(f"[{args.target}] Using monitor_target from {origin}")
    except Exception as e:
        logger.error(f"[{args.target}] {e}")
        return 1

    # 5) Call the monitor loop
    try:
        monitor_target(
            ip=args.target,
            source_ip=args.source,
            settings=settings,
            logger=logger,
        )
        logger.info(f"[{args.target}] Monitor exited normally.")
        return 0

    except KeyboardInterrupt:
        logger.info(f"[{args.target}] Stopped by user (KeyboardInterrupt).")
        return 0

    except SystemExit as e:
        code = int(getattr(e, "code", 0) or 0)
        if code == 0:
            logger.info(f"[{args.target}] Monitor requested clean exit.")
        else:
            logger.warning(f"[{args.target}] Monitor requested exit with code {code}.")
        return code

    except Exception as e:
        logger.exception(f"[{args.target}] Monitor crashed: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
