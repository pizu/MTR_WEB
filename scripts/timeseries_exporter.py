#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/timeseries_exporter.py
==============================

Purpose
-------
Export Chart.js-friendly JSON time-series bundles from per-target RRD files
into <paths.html>/data for each target IP and configured time range.

Inputs
------
- Settings YAML (via --settings), providing:
    paths.html, paths.rrd (required)
    paths.traceroute (strict read-only for labels)
    graph_time_ranges: list/dict of {label, seconds}
- Targets YAML (auto-resolved by utils.resolve_targets_path(settings)),
  supporting multiple shapes (see _load_targets_from_file).

Outputs
-------
<paths.html>/data/<ip>_<label>.json  for each (ip, time-range)

CLI
---
--settings PATH   : path to mtr_script_settings.yaml (default: mtr_script_settings.yaml)
--ip IP           : export only this IP (skip targets file)
--label LABEL     : export only this time range (e.g., "1h")
--dry-run         : log actions without writing

Exit codes
----------
0 on success, 1 on configuration/parse errors.
"""

from __future__ import annotations

import os
import sys
import argparse
from typing import Dict, Any, List, Optional, Tuple

import yaml

from modules.utils import (
    load_settings,
    setup_logger,
    resolve_all_paths,
    resolve_html_dir,
    resolve_targets_path,
    get_html_ranges,
)
from modules.rrd_exporter import export_ip_timerange_json


# -----------------------------------------------------------------------------
# Target parsing helpers
# -----------------------------------------------------------------------------

def _normalize_target_row(row) -> Optional[Dict[str, Any]]:
    """
    Normalize a row into: {"ip": <str>, "description": <str>, "pause": <bool>}

    Accepted shapes:
      - dict: {"ip": "8.8.8.8", "description": "...", "pause": false}
      - str: "8.8.8.8"
      - list/tuple:
            ["8.8.8.8"]                           -> ip only
            ["8.8.8.8", "Google DNS"]             -> ip + description
            ["8.8.8.8", "Google DNS", true]       -> ip + description + pause
            ["8.8.8.8", true]                     -> ip + pause
    """
    if isinstance(row, dict):
        ip = str(row.get("ip") or "").strip()
        if not ip:
            return None
        desc = str(row.get("description") or row.get("desc") or "").strip()
        pause = bool(row.get("pause") or row.get("paused") or (not row.get("enabled", True)))
        return {"ip": ip, "description": desc, "pause": pause}

    if isinstance(row, str):
        ip = row.strip()
        if not ip:
            return None
        return {"ip": ip, "description": "", "pause": False}

    if isinstance(row, (list, tuple)):
        if not row:
            return None
        ip = str(row[0]).strip()
        if not ip:
            return None
        desc = ""
        pause = False
        if len(row) >= 2:
            if isinstance(row[1], str):
                desc = row[1].strip()
            else:
                pause = bool(row[1])
        if len(row) >= 3 and isinstance(row[2], (bool, int)):
            pause = bool(row[2])
        return {"ip": ip, "description": desc, "pause": pause}

    return None


def _load_targets_from_file(path: str, logger) -> List[Dict[str, Any]]:
    """
    Parse the targets YAML and return a list of ACTIVE targets (pause==False).

    Supports:
      - list of items (dicts/strings/lists as described above)
      - mapping: ip -> dict|str|list
    De-duplicates by IP (first definition wins).
    """
    if not os.path.isfile(path):
        logger.warning(f"Targets file not found: {path}")
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    normalized: List[Dict[str, Any]] = []
    seen_ips = set()

    if isinstance(data, list):
        for row in data:
            t = _normalize_target_row(row)
            if not t:
                logger.debug(f"[targets] skipped unparsable row: {row!r}")
                continue
            ip = t["ip"]
            if ip in seen_ips:
                continue
            seen_ips.add(ip)
            normalized.append(t)

    elif isinstance(data, dict):
        for key, row in data.items():
            candidate = None
            if isinstance(row, dict) and row.get("ip"):
                candidate = _normalize_target_row(row)
            else:
                if isinstance(row, dict):
                    row2 = dict(row)
                    row2.setdefault("ip", str(key))
                    candidate = _normalize_target_row(row2)
                elif isinstance(row, (list, tuple, str)):
                    candidate = _normalize_target_row(row)
                    if candidate and not candidate.get("ip"):
                        candidate["ip"] = str(key)
                else:
                    candidate = {"ip": str(key), "description": "", "pause": False}

            if not candidate:
                logger.debug(f"[targets] skipped unparsable mapping entry: {key!r}: {row!r}")
                continue

            ip = candidate["ip"]
            if ip in seen_ips:
                continue
            seen_ips.add(ip)
            normalized.append(candidate)
    else:
        logger.error(f"[targets] Unsupported YAML root type: {type(data).__name__}")
        return []

    active = [t for t in normalized if not t.get("pause")]
    if not active:
        logger.warning("Targets parsed successfully but all are paused or empty.")
    return active


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

    # Time ranges
    ranges = get_html_ranges(settings)
    if args.label:
        ranges = [r for r in ranges if r.get("label") == args.label]
        if not ranges:
            logger.error(f"No matching time range label: {args.label}")
            return 1

    if not ranges:
        logger.warning("No time ranges configured; nothing to export.")
        return 0

    # IP list
    ef _discover_rrd_ips_from_dir(rrd_dir: str):
    if not rrd_dir or not os.path.isdir(rrd_dir):
        return []
    return sorted({os.path.splitext(n)[0] for n in os.listdir(rrd_dir) if n.endswith(".rrd")})

if args.ip:
    ips = [args.ip]
else:
    ips = [t.get("ip") for t in targets if t.get("ip") and t["ip"] != "mtr_settings"]
    seen = set()
    ips = [ip for ip in ips if not (ip in seen or seen.add(ip))]
    if not ips:
        from modules.utils import resolve_all_paths
        rrd_dir = resolve_all_paths(settings)["rrd"]
        ips = _discover_rrd_ips_from_dir(rrd_dir)
        if ips:
            logger.warning(f"No valid targets; falling back to RRD scan ({len(ips)} ip(s)).")
        else:
            logger.warning("No targets and no RRDs found; nothing to export.")
            return 0


    out_dir = os.path.join(html_dir, "data")
    logger.info(f"Exporting {len(ips)} IP(s) over {len(ranges)} time range(s) into {out_dir}")

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
