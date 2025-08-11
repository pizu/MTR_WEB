#!/usr/bin/env python3
"""
 mtr_watchdog.py

 CLI entrypoint that prepares logging and delegates to the long-running
 monitor loop (modules/monitor.py: monitor_target). This script does not run
 MTR directly; it wires arguments and logging, then hands off.
"""

# Standard library imports for path handling and argument parsing
import os
import sys
import argparse

# Ensure the 'modules' package is importable when running from the scripts/ dir
sys.path.insert(0, os.path.dirname(__file__))

# Project utilities for loading settings and creating consistent loggers
from modules.utils import load_settings, setup_logger

# The monitor function that contains the 'while True' loop and post-processing
from modules.monitor import monitor_target

# -----------------------------
# Argument parsing (CLI flags)
# -----------------------------
parser = argparse.ArgumentParser(description="Launch MTR monitoring for a target.")
parser.add_argument("--settings", default="mtr_script_settings.yaml",
                    help="Path to the YAML settings file (default: mtr_script_settings.yaml)")
parser.add_argument("--target", required=True,
                    help="Destination host/IP to monitor (required)")
parser.add_argument("--source",
                    help="Optional source IP address to bind (passed to mtr --address)")

# Parse the command-line arguments into an 'args' namespace
args = parser.parse_args()

# -----------------------------
# Logger initialization
# -----------------------------
# Load settings once here just to initialize logging; the monitor loop will
# re-load YAML each cycle to apply changes live (interval, loop_enabled, etc.).
settings = load_settings(args.settings)

# Determine where logs should be written; default to /tmp if not defined
log_directory = settings.get("log_directory", "/tmp")

# Create a logger named 'mtr_watchdog' that writes to a main file AND a per-target file
# The 'extra_file' provides an additional handler pointing to TARGET.log for easy grepping
logger = setup_logger(
    "mtr_watchdog",
    log_directory,
    "mtr_watchdog.log",
    settings=settings,
    extra_file=f"{args.target}.log",
)

# -----------------------------
# Handoff to the monitor loop
# -----------------------------
try:
    # Pass the settings path, not a static dict, so the monitor can live-reload YAML
    monitor_target(
     ip=args.target,
     source_ip=args.source,
     settings=settings,
     logger=logger,
    )

except KeyboardInterrupt:
    # Clean, intentional shutdown (Ctrl+C); useful when running interactively
    logger.info(f"[{args.target}] Stopped by user.")

except Exception as e:
    # Any unexpected crash coming from the monitor is logged here once
    logger.exception(f"[{args.target}] Monitor crashed: {e}")
    raise  # Re-raise so supervisors (systemd, controller) can react if needed
