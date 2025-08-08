#!/usr/bin/env python3
"""
mtr_watchdog.py — Entrypoint for launching MTR monitoring for a single target.
This script loads settings, initializes logging, and starts the monitoring loop.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # Add current dir to sys.path

import argparse  # Used for parsing command-line arguments
from modules.utils import load_settings, setup_logger  # Your utility functions to load YAML + logging setup
from modules.monitor import monitor_target  # The core monitoring logic

# ----------------------
# Parse command-line args
# ----------------------
parser = argparse.ArgumentParser(description="Launch MTR monitoring for a target.")

# --settings is optional, defaults to mtr_script_settings.yaml
parser.add_argument("--settings", default="mtr_script_settings.yaml", help="Path to settings YAML")

# --target is REQUIRED (e.g., 8.8.8.8)
parser.add_argument("--target", required=True, help="Target IP or hostname to monitor")

# --source is optional: you can specify a source IP for outbound MTR
parser.add_argument("--source", help="Optional source IP for MTR probing")

args = parser.parse_args()  # Parse the arguments from the command line

# ----------------------
# Load YAML settings and setup logging
# ----------------------
settings = load_settings(args.settings)  # Load YAML settings (intervals, RRD paths, etc.)
log_directory = settings.get("log_directory", "/tmp")  # Fallback to /tmp if not set

# Initialize logger with target-specific log file
logger = setup_logger("mtr_watchdog", log_directory, "mtr_watchdog.log", settings=settings, extra_file=f"{args.target}.log")

# ----------------------
# Run the monitor loop for this target
# ----------------------
monitor_target(args.target, args.source, settings, logger)
