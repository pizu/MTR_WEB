#!/usr/bin/env python3
"""
timeseries_exporter.py
======================
Exports Chart.js‑friendly JSON time‑series bundles for each target & time range:
  html/data/<ip>_<rangeLabel>.json

CLI compatibility:
- New style:  --settings /path/to/mtr_script_settings.yaml
- Legacy:     timeseries_exporter.py /path/to/mtr_script_settings.yaml
- Default:    repo root ../mtr_script_settings.yaml when no args provided

Exit codes:
- 0 on success (even if some targets/ranges fail; those failures are logged)
- 1 for fatal launcher errors (settings unreadable, targets file unreadable, etc.)
"""

import os
import sys
import argparse
import yaml
from modules.utils import load_settings, setup_logger, resolve_targets_path, get_html_ranges  # add get_html_ranges

# --- make scripts/modules importable (works via systemd and shell) ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
for p in (MODULES_DIR, SCRIPTS_DIR, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from modules.rrd_exporter import export_ip_timerange_json                   # noqa: E402


def resolve_settings_path(default_name: str = "mtr_script_settings.yaml") -> str:
    """
    Resolve settings path compatibly:
      1) --settings <path>
      2) first positional (legacy)
      3) ../mtr_script_settings.yaml
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--settings", dest="settings", default=None)
    known, _ = parser.parse_known_args()
    if known.settings and known.settings != "--settings":
        return os.path.abspath(known.settings)
    for tok in sys.argv[1:]:
        if not tok.startswith("-"):
            return os.path.abspath(tok)
    return os.path.abspath(os.path.join(REPO_ROOT, default_name))


def main() -> int:
    # 1) Settings + logger
    settings_path = resolve_settings_path()
    try:
        settings = load_settings(settings_path)
    except Exception as e:
        print(f"[FATAL] Failed to load settings '{settings_path}': {e}", file=sys.stderr)
        return 1

    logger = setup_logger("timeseries_exporter", settings=settings)

    # 2) Load targets (repo root)
    targets_file = resolve_targets_path()
    try:
        with open(targets_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        targets = data.get("targets", []) or []
        logger.info(f"Loaded {len(targets)} targets from {targets_file}")
    except Exception as e:
        logger.error(f"Failed to load targets from {targets_file}: {e}")
        return 1

    # 3) Time ranges
    ranges = [r for r in (get_html_ranges(settings) or []) if r.get("label") and r.get("seconds")]

    if not ranges:
        logger.warning("No graph_time_ranges in settings; nothing to export.")
        return 0

    # 4) Export
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
