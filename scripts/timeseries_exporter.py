#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/timeseries_exporter.py
==============================

Purpose
-------
Export Chart.js-friendly JSON bundles from per-target RRD files into:

    <paths.html>/data/<ip>_<label>.json

for each active target IP and each configured time range label.

Where the data comes from
-------------------------
- Per-target RRD files live under <paths.rrd> and contain DS per hop+metric,
  e.g. "hop0_avg", "hop3_loss", etc.
- Time ranges (labels + seconds) are read from:
    settings['html']['time_ranges']   # preferred
  with backwards compatibility for legacy keys (handled by utils.get_html_ranges).

Target list inputs (flexible)
-----------------------------
- The targets YAML file is auto-resolved via utils.resolve_targets_path(settings).
  It supports the following shapes (mix and match as needed):

  1) List of dicts/strings/tuples:
     - {"ip": "8.8.8.8", "description": "Google", "pause": false}
     - "1.1.1.1"
     - ["9.9.9.9"]                       -> ip only
     - ["9.9.9.9", "Quad9"]              -> ip + description
     - ["9.9.9.9", true]                 -> ip + pause
     - ["9.9.9.9", "Quad9", true]        -> ip + description + pause

  2) Mapping form (ip -> item), value can be dict/list/string (ip inferred from key).

- Paused targets are ignored.
- The special pseudo-target "mtr_settings" (if present) is **ignored** here.

CLI
---
--settings PATH   : path to mtr_script_settings.yaml (default: mtr_script_settings.yaml)
--ip IP           : export only this IP (overrides targets file)
--label LABEL     : export only this time range label (e.g. "1h")
--dry-run         : log actions without writing any files

Exit codes
----------
0 on success; 1 on configuration/parse errors.

Logging policy compliance
-------------------------
- Uses modules.utils.setup_logger("timeseries_exporter", settings) to inherit:
  - rotating file handler at <paths.logs>/timeseries_exporter.log
  - console handler (stdout) with the same format
- On settings load failure (before we can call setup_logger), a _bootstrap logger
  is used to emit a proper "[FATAL]" style error to stderr with the same format.
- Immediately calls modules.utils.refresh_logger_levels(...) after settings load
  so updated YAML levels take effect.

Notes
-----
- This script only *reads* RRDs and writes JSON; it does not create RRD files.
  Make sure your monitor/watchdog pipeline is populating <paths.rrd>.
"""

from __future__ import annotations

import os
import sys
import argparse
import logging
from typing import Dict, Any, List, Optional

import yaml

# Project helpers
from modules.utils import (
    load_settings,
    setup_logger,
    refresh_logger_levels,
    resolve_all_paths,
    resolve_html_dir,
    resolve_targets_path,
    get_html_ranges,
)
from modules.rrd_exporter import export_ip_timerange_json


# -----------------------------------------------------------------------------
# Bootstrap logger (used ONLY if settings fail to load)
# -----------------------------------------------------------------------------
def _bootstrap_logger() -> logging.Logger:
    """
    Create a temporary stderr logger with the same format as utils.setup_logger
    for early-failure reporting (before settings are available).
    """
    lg = logging.getLogger("timeseries_exporter_bootstrap")
    if lg.handlers:
        return lg
    lg.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=sys.stderr)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    lg.addHandler(handler)
    lg.propagate = False
    return lg


# -----------------------------------------------------------------------------
# Target parsing helpers
# -----------------------------------------------------------------------------

def _normalize_target_row(row) -> Optional[Dict[str, Any]]:
    """
    Normalize a row into: {"ip": <str>, "description": <str>, "pause": <bool>}

    Accepted shapes:
      - dict: {"ip": "8.8.8.8", "description": "...", "pause": false}
              (also accepts "desc", "paused", "enabled" for pragmatism)
      - str:  "8.8.8.8"
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
      - mapping: ip -> dict|str|list (ip inferred if missing)
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
        items = enumerate(data)
    elif isinstance(data, dict):
        items = data.items()
    else:
        logger.error(f"[targets] Unsupported YAML root type: {type(data).__name__}")
        return []

    for key, row in items:
        if isinstance(data, dict):
            # mapping form: ip -> row
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
        else:
            candidate = _normalize_target_row(row)

        if not candidate:
            logger.debug(f"[targets] skipped unparsable entry: {key!r}: {row!r}")
            continue

        ip = candidate["ip"]
        if ip in seen_ips:
            continue
        seen_ips.add(ip)
        normalized.append(candidate)

    active = [t for t in normalized if not t.get("pause")]
    if not active:
        logger.warning("Targets parsed successfully but all are paused or empty.")
    return active


def _discover_rrd_ips_from_dir(rrd_dir: str) -> List[str]:
    """
    Fallback: scan <paths.rrd> for '*.rrd' and return IPs derived from filenames.
    """
    if not rrd_dir or not os.path.isdir(rrd_dir):
        return []
    return sorted({
        os.path.splitext(name)[0]
        for name in os.listdir(rrd_dir)
        if name.endswith(".rrd")
    })


def _resolve_ip_list(settings: Dict[str, Any], args, logger) -> List[str]:
    """
    Build the list of IPs to export:

      - If --ip is given: [args.ip]
      - Else: read targets from the targets YAML and exclude the pseudo 'mtr_settings'
      - If that yields nothing: fall back to scanning <paths.rrd> for '*.rrd'
    """
    if args.ip:
        return [args.ip]

    targets_path = resolve_targets_path(settings)
    targets = _load_targets_from_file(targets_path, logger)

    ips = [t.get("ip") for t in targets if t.get("ip") and t["ip"] != "mtr_settings"]

    # uniq while preserving order
    seen = set()
    ips = [ip for ip in ips if not (ip in seen or seen.add(ip))]

    if ips:
        return ips

    # Fallback: scan the RRD directory
    rrd_dir = resolve_all_paths(settings)["rrd"]
    ips = _discover_rrd_ips_from_dir(rrd_dir)
    if ips:
        logger.warning(f"No valid targets in {targets_path}; falling back to RRD scan ({len(ips)} ip(s)).")
    return ips


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export RRD time-series JSON bundles")
    ap.add_argument("--settings", default="mtr_script_settings.yaml",
                    help="Path to YAML settings (default: mtr_script_settings.yaml)")
    ap.add_argument("--ip", help="Export only this IP")
    ap.add_argument("--label", help="Export only this time-range label (e.g. '1h')")
    ap.add_argument("--dry-run", action="store_true", help="Log actions without writing files")
    args = ap.parse_args(argv)

    # Settings + logger
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        boot = _bootstrap_logger()
        boot.error(f"[FATAL] cannot load settings: {e}")
        return 1

    # Create the standard logger using your central utilities
    logger = setup_logger("timeseries_exporter", settings=settings)

    # Immediately refresh levels from YAML in case they changed
    try:
        refresh_logger_levels(settings, ["timeseries_exporter", "modules", "paths"])
    except Exception:
        # Non-fatal; continue with current levels
        pass

    # Ensure output directory root exists (and compute all paths)
    paths = resolve_all_paths(settings)
    html_dir = resolve_html_dir(settings)  # ensures <paths.html> exists

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

    # IP list (honor --ip, else targets file, else RRD scan)
    ips = _resolve_ip_list(settings, args, logger)
    if not ips:
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
