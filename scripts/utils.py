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
"""

import os
import yaml
import logging

def load_settings(settings_path=None):
    if settings_path is None:
        settings_path = os.path.join(os.path.dirname(__file__), '../mtr_script_settings.yaml')
    with open(settings_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logger(name, log_directory, log_filename):
    os.makedirs(log_directory, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_directory, log_filename)
    if not logger.handlers:
        handler = logging.FileHandler(log_path)
        handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(handler)

    return logger
