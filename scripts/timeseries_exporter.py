#!/usr/bin/env python3
"""
timeseries_exporter.py

Generates Chart.js-friendly JSON files for each target & time range:
  html/data/<ip>_<rangeLabel>.json

Run this before html_generator.py (or add as a step in your controller/cron).
"""

import os, yaml, sys
from modules.utils import load_settings, setup_logger
from modules.rrd_exporter import export_ip_timerange_json

def main():
  return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))
  settings_path = sys.argv[1] if len(sys.argv) > 1 else _default_settings_path()
  settings = load_settings(settings_path)
  
    logger = setup_logger("timeseries_exporter", settings.get("log_directory", "/tmp"),
                          "timeseries_exporter.log", settings=settings)

    # Load targets
    try:
        with open("mtr_targets.yaml") as f:
            targets = yaml.safe_load(f).get("targets", [])
        logger.info(f"Loaded {len(targets)} targets")
    except Exception as e:
        logger.error(f"Failed to load targets: {e}")
        targets = []

    ranges = [r for r in settings.get("graph_time_ranges", []) if r.get("label") and r.get("seconds")]
    if not ranges:
        logger.warning("No graph_time_ranges in settings; nothing to export.")
        return

    total = 0
    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue
        for rng in ranges:
            try:
                export_ip_timerange_json(ip, settings, rng["label"], int(rng["seconds"]), logger=logger)
                total += 1
            except Exception as e:
                logger.warning(f"[{ip}] export failed for {rng['label']}: {e}")
    logger.info(f"Done. JSON bundles generated: {total}")

if __name__ == "__main__":
    main()
