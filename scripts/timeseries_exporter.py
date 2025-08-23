#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/timeseries_exporter.py
==============================

Purpose
-------
Export Chart.js-friendly JSON time-series for each target IP and time range
into <paths.html>/data, using the per-target RRD and hop labels/cache.

Data source
-----------
Per-target RRDs must live under settings['paths']['rrd'] as <ip>.rrd.

Outputs
-------
For each (ip, range), write:
    <paths.html>/data/<ip>_<label>.json

Configuration
-------------
- YAML: mtr_script_settings.yaml (path passed via --settings)
  Required paths:
    paths.html, paths.rrd
  Optional:
    paths.logs, paths.graphs, paths.cache
  Strict:
    paths.traceroute  (used by exporter to read hop labels; not created)

- Targets file (YAML):
  Auto-resolved by utils.resolve_targets_path(settings):
    1) settings['files']['targets'] if set (resolved relative to settings file)
    2) ./mtr_targets.yaml next to the settings file
    3) ./mtr_targets.yaml in CWD

Accepted shapes:
  - list of dicts: [{'ip':'1.1.1.1', 'description':'...', 'pause':false}, ...]
  - mapping: {'1.1.1.1': {'description':'...', 'pause':false}, ...}

CLI
---
--settings PATH    : YAML settings path (default: mtr_script_settings.yaml)
--ip IP            : export only this single IP
--label LABEL      : export only this time range label (e.g., '1h')
--dry-run          : log actions without writing files

Notes
-----
- If --ip is omitted, targets are loaded from the targets YAML.
- If --label is omitted, time ranges come from settings['graph_time_ranges'].
- The exporter will log and skip if the RRD is missing.

"""

from __future__ import annotations

import os
import sys
import argparse
from typing import Dict, Any, List

import yaml

from modules.utils import (
    load_settings,
    setup_logger,
    resolve_all_paths,
    resolve_html_dir,
    resolve_targets_path,  # IMPORTANT: pass settings when calling
    get_html_ranges,
)

from modules.rrd_exporter import export_ip_timerange_json


# -----------------------------------------------------------------------------
# Targets loading
# -----------------------------------------------------------------------------

def _load_targets_from_file(path: str) -> List[Dict[str, Any]]:
    """
    Parse a targets YAML file and return a list of active targets:
      [{'ip': '1.1.1.1', 'description': '...', 'pause': False}, ...]
    """
    if not os.path.isfile(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    out: List[Dict[str, Any]] = []

    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            ip = str(row.get("ip") or "").strip()
            if not ip:
                continue
            out.append({
                "ip": ip,
                "description": str(row.get("description") or ""),
                "pause": bool(row.get("pause") or row.get("paused") or False),
            })

    elif isinstance(data, dict):
        for ip, row in data.items():
            if not ip:
                continue
            row = row or {}
            out.append({
                "ip": str(ip).strip(),
                "description": str(row.get("description") or ""),
                "pause": bool(row.get("pause") or row.get("paused") or False),
            })

    # Drop paused
    return [t for t in out if not t.get("pause")]


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export RRD time-series JSON bundles")
    ap.add_argument("--settings", default="mtr_script_settings.yaml",
                    help="Path to YAML settings (default: mtr_script_settings.yaml)")
    ap.add_argument("--ip", help="Export only this IP")
    ap.add_argument("--label", help="Export only this time range label (e.g. '1h')")
    ap.add_argument("--dry-run", action="store_true", help="Log actions without writing files")
    args = ap.parse_args(argv)

    # Settings + logger
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        print(f"[FATAL] cannot load settings: {e}", file=sys.stderr)
        return 1

    logger = setup_logger("timeseries_exporter", settings=settings)
    paths = resolve_all_paths(settings)
    html_dir = resolve_html_dir(settings)  # ensures <html> exists

    # Determine time ranges
    ranges = get_html_ranges(settings)
    if args.label:
        ranges = [r for r in ranges if r.get("label") == args.label]
        if not ranges:
            logger.error(f"No matching time range label: {args.label}")
            return 1

    # Determine IPs
    ips: List[str]
    if args.ip:
        ips = [args.ip]
    else:
        targets_file = resolve_targets_path(settings)  # FIX: pass settings
        targets = _load_targets_from_file(targets_file)
        ips = [t["ip"] for t in targets]

    if not ips:
        logger.warning("No targets to export (no --ip given and targets file empty/missing).")
        return 0

    if not ranges:
        logger.warning("No time ranges configured; nothing to export.")
        return 0

    logger.info(f"Exporting {len(ips)} IP(s) over {len(ranges)} time range(s) into {os.path.join(html_dir, 'data')}")

    # Export
    total = 0
    for ip in ips:
        for r in ranges:
            label = r["label"]
            seconds = int(r["seconds"])
            logger.info(f"[{ip}] {label} ({seconds}s)")

            if args.dry_run:
                continue

            out_path = export_ip_timerange_json(ip, settings, label, seconds, logger=logger)
            logger.debug(f"[{ip}] wrote {out_path}")
            total += 1

    logger.info(f"Done. Exported {total} bundle(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
