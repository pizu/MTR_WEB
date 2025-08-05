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
        settings_path = os.path.join(os.path.dirname(__file__), '../mtr_script_settings.yaml')
    with open(settings_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logger(name, log_directory, log_filename, settings=None, default_level="INFO"):
    """
    Sets up a logger with file and console handlers and a per-script level.

    :param name: Logger name (e.g. "controller", "mtr_monitor")
    :param log_directory: Directory for log files
    :param log_filename: Log filename (e.g. controller.log)
    :param settings: Full settings dict, used to extract per-script level
    :param default_level: Fallback log level
    :return: Logger object
    """
    os.makedirs(log_directory, exist_ok=True)
    logger = logging.getLogger(name)

    # Prevent duplicate handlers if reused
    if logger.hasHandlers():
        return logger

    # Determine log level
    level_str = default_level
    if settings and "logging_levels" in settings:
        level_str = settings["logging_levels"].get(name, default_level)
    log_level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    # File handler
    file_path = os.path.join(log_directory, log_filename)
    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
