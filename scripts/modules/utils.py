#!/usr/bin/env python3
"""
utils.py - Shared utility functions for the MTR monitoring system.

This module provides:
- load_settings(): Load settings from mtr_script_settings.yaml
- setup_logger(): Standardized logger initialization for scripts

Expected keys in mtr_script_settings.yaml:

log_directory: "/opt/scripts/MTR_WEB/logs/"        # Directory where all script logs are written
interval_seconds: 60                               # Time between each MTR probe run
loss_threshold: 10                                 # % packet loss to trigger alerts
debounce_seconds: 300                              # Time to suppress repeated alerts
retention_days: 30                                 # (Optional) How long to keep old RRD/log data
log_lines_display: 100                             # How many log lines to show on HTML per target
logging_levels:                                     # Per-script log levels (optional)
  controller: INFO
  mtr_monitor: DEBUG
  graph_generator: WARNING
  html_generator: INFO
  index_generator: INFO
"""

import os
import yaml
import logging

def load_settings(settings_path=None):
    if settings_path is None:
        # Try ../mtr_script_settings.yaml
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        settings_path = os.path.join(base_dir, "mtr_script_settings.yaml")
    with open(settings_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logger(name, log_directory, log_filename, settings=None, default_level="INFO", extra_file=None):
    """
    Sets up a logger with file and console handlers and a per-script level.

    :param name: Logger name (e.g. "mtr_monitor", "8.8.8.8")
    :param log_directory: Directory for log files
    :param log_filename: Primary log filename (e.g. mtr_monitor.log)
    :param settings: Full settings dict, used to extract per-script level
    :param default_level: Fallback log level
    :param extra_file: Optional additional file (e.g., per-target log)
    :return: Logger object
    """
    os.makedirs(log_directory, exist_ok=True)
    logger = logging.getLogger(name)
    logger.propagate = False  # Avoid duplicate log prints

    # Prevent duplicate handlers if reused
    if logger.hasHandlers():
        return logger

    # Determine log level
    level_str = default_level
    if settings and "logging_levels" in settings:
        level_str = settings["logging_levels"].get(name, default_level)
    log_level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(logging.DEBUG)  # Always accept all; handlers will filter

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    # Main file handler
    file_path = os.path.join(log_directory, log_filename)
    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    logger.addHandler(file_handler)

    # Optional per-target file handler
    if extra_file:
        extra_path = os.path.join(log_directory, extra_file)
        extra_handler = logging.FileHandler(extra_path)
        extra_handler.setFormatter(formatter)
        extra_handler.setLevel(log_level)
        logger.addHandler(extra_handler)

    # Console handler (optional; comment out if running in cron/service)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    return logger
