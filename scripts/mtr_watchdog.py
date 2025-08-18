#!/usr/bin/env python3
"""
mtr_watchdog.py

CLI entrypoint that:
  1) parses arguments (settings path, target, optional source),
  2) loads settings to initialize logging,
  3) creates a main logger + per-target extra log,
  4) hands off to the monitor loop (modules.monitor.monitor_target).

This file does NOT run MTR directly; the monitor loop calls run_mtr()
every cycle and handles RRD/traceroute/HTML/logging decisions.
"""

import os
import sys
import argparse

# Ensure 'modules' is importable when running from the scripts/ directory
sys.path.insert(0, os.path.dirname(__file__))

from modules.utils import load_settings, setup_logger, refresh_logger_levels
from modules.monitor import monitor_target  # the long-running loop

# -----------------------------
# Argument parsing (CLI flags)
# -----------------------------
parser = argparse.ArgumentParser(description="Launch MTR monitoring for a target.")
parser.add_argument("--settings", default="mtr_script_settings.yaml",
                    help="Path to YAML settings (default: mtr_script_settings.yaml)")
parser.add_argument("--target", required=True,
                    help="Destination host/IP to monitor")
parser.add_argument("--source",
                    help="Optional source IP address to bind (passed to mtr --address)")
args = parser.parse_args()

# -----------------------------
# Initialize logger
# -----------------------------
# Load settings ONCE here to configure logging. (The monitor can re-read YAML each loop if desired.)
settings = load_settings(args.settings)

# Where to write logs (falls back to /tmp if not set).
refresh_logger_levels(logger, "mtr_watchdog", settings)
log_directory = settings.get("log_directory", "/tmp")

# Create a logger named 'mtr_watchdog'.
# IMPORTANT: setup_logger expects first three args POSITIONALLY: (name, log_dir, filename).
logger = setup_logger(
    "mtr_watchdog",
    log_directory,
    "mtr_watchdog.log",
    settings=settings,
    # Also log to a per-target file like "8.8.8.8.log" for easier grepping.
    extra_file=f"{args.target}.log",
)

# -----------------------------
# Handoff to the monitor loop
# -----------------------------
try:
    # Call your monitor with the CURRENT signature:
    # def monitor_target(ip, source_ip, settings, logger):
    monitor_target(
        ip=args.target,
        source_ip=args.source,
        settings=settings,  # pass dict; if you want live reload, implement it in monitor.py
        logger=logger,
    )

except KeyboardInterrupt:
    # Clean stop (Ctrl+C), common when running interactively.
    logger.info(f"[{args.target}] Stopped by user.")

except Exception as e:
    # Any unexpected crash bubbles up here; log once and re-raise if you want a supervisor to restart it.
    logger.exception(f"[{args.target}] Monitor crashed: {e}")
    raise
