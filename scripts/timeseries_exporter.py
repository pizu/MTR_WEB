#!/usr/bin/env python3
"""
timeseries_exporter.py

Generates Chart.js-friendly JSON files for each target & time range:
  html/data/<ip>_<rangeLabel>.json

This script accepts the absolute settings path as argv[1] (passed by controller.py).
If not provided, it falls back to ../mtr_script_settings.yaml relative to this file.
"""

import os
import sys
import yaml
from modules.utils import load_settings, setup_logger
from modules.rrd_exporter import export_ip_timerange_json

def _default_settings_path() -> str:
    """Return the repo-root mtr_script_settings.yaml (../ from scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))

def _targets_path() -> str:
    """Return the repo-root mtr_targets.yaml (../ from scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_targets.yaml"))

def main() -> None:
    # 1) Load settings (absolute path from controller if provided)
    settings_path = sys.argv[1] if len(sys.argv) > 1 else _default_settings_path()
    settings = load_settings(settings_path)

    # 2) Logger
    logger = setup_logger(
        "timeseries_exporter",
        settings.get("log_directory", "/tmp"),
        "timeseries_exporter.log",
        settings=settings
    )

    # 3) Load targets from repo root (not CWD)
    targets_file = _targets_path()
    try:
        with open(targets_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        targets = data.get("targets", []) or []
        logger.info(f"Loaded {len(targets)} targets from {targets_file}")
    except Exception as e:
        logger.error(f"Failed to load targets from {targets_file}: {e}")
        return

    # 4) Ranges to export (from YAML config)
    ranges = [r for r in (settings.get("graph_time_ranges") or []) if r.get("label") and r.get("seconds")]
    if not ranges:
        logger.warning("No graph_time_ranges in settings; nothing to export.")
        return

    # 5) Export bundles
    total = 0
    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue
        for rng in ranges:
            label = rng["label"]
            seconds = int(rng["seconds"])
            try:
                export_ip_timerange_json(ip, settings, label, seconds, logger=logger)
                total += 1
            except Exception as e:
                logger.warning(f"[{ip}] export failed for {label}: {e}")

    logger.info(f"Done. JSON bundles generated: {total}")

if __name__ == "__main__":
    main()
