#!/usr/bin/env python3
"""
cleanup.py
Deletes old RRD, log, traceroute, graph, and HTML files based on per-type retention defined in mtr_script_settings.yaml.
Uses shared logger from utils.py.
"""

import os
import sys
import time
from datetime import datetime

# Ensure local imports work no matter the CWD
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "modules"))

from modules.utils import load_settings, setup_logger

# Load settings
settings = load_settings("mtr_script_settings.yaml")

# Logger
logger = setup_logger("cleanup", settings.get("log_directory", "/tmp"), "cleanup.log", settings=settings)

# Directories from settings
RRD_DIR         = settings.get("rrd_directory", "data")
TRACEROUTE_DIR  = settings.get("traceroute_directory", "traceroute")
GRAPH_DIR       = settings.get("graph_output_directory", "html/graphs")
HTML_DIR        = "html"
LOG_DIR         = settings.get("log_directory", "logs")

# Retention days per type
retention = settings.get("retention", {})
now = time.time()

def cleanup_dir(path, days, extensions=None, label=None):
    if not days or days <= 0:
        logger.info(f"[SKIP] Retention for {label or path} is 0 or not set.")
        return
    if not os.path.isdir(path):
        logger.info(f"[SKIP] {label or path}: directory does not exist.")
        return

    cutoff = now - (days * 86400)
    deleted = 0
    for root, _, files in os.walk(path):
        for file in files:
            if extensions and not file.endswith(tuple(extensions)):
                continue
            file_path = os.path.join(root, file)
            try:
                if os.path.getmtime(file_path) < cutoff:
                    os.remove(file_path)
                    deleted += 1
                    logger.info(f"[{label}] Deleted: {file_path}")
            except Exception as e:
                logger.warning(f"[{label}] Could not delete {file_path}: {e}")

    logger.info(f"[{label}] Deleted {deleted} file(s) older than {days} days.")

def main():
    logger.info("===== Cleanup started =====")
    cleanup_dir(RRD_DIR,        retention.get("rrd_days"),        [".rrd"],  "RRD files")
    cleanup_dir(LOG_DIR,        retention.get("logs_days"),       [".log"],  "Logs")
    cleanup_dir(TRACEROUTE_DIR, retention.get("traceroute_days"), [".json"], "Traceroutes")
    cleanup_dir(GRAPH_DIR,      retention.get("graphs_days"),     [".png"],  "Graphs")
    cleanup_dir(HTML_DIR,       retention.get("html_days"),       [".html"], "HTML pages")
    logger.info("===== Cleanup finished =====")

if __name__ == "__main__":
    main()
