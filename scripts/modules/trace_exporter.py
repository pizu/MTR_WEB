#!/usr/bin/env python3
"""
modules/trace_exporter.py

Writes traceroute artifacts for a target:

1) <ip>.trace.txt
   Human-readable lines: "<hop> <ip/host> <avg> ms"
   (Hop 0 is ignored; only hops >= 1 are written.)

2) <ip>.json
   Legacy map: {"hop1":"1.2.3.4", "hop2":"5.6.7.8", ...}
   (Still emitted for backward compatibility.)

3) <ip>_hops.json
   Stabilized labels (including "varies (...)")
   NOTE: This file is produced from *existing* rolling stats and does not
   update stats here (to avoid double counting). Stats are maintained elsewhere.

Traceroute path resolution
--------------------------
Uses modules.utils.resolve_all_paths(settings) to honor your YAML:
  settings['paths']['traceroute']  (preferred)
with fallbacks:
  env MTR_TRACEROUTE_DIR
  settings['paths']['traces']      (legacy)
  /opt/scripts/MTR_WEB/traceroute
  /opt/scripts/MTR_WEB/traces

This module never creates its own logger.
"""

import os
import json
from typing import Dict

from modules.utils import resolve_all_paths

# ---- tuning knobs ----
UNSTABLE_THRESHOLD = 0.45   # if top host share < 45% → label as "varies(...)"
TOPK_TO_SHOW       = 3      # list up to 3 hosts inside "varies(...)"
MAJORITY_WINDOW    = 200    # soft cap on total samples kept per hop (decay oldest)
STICKY_MIN_WINS    = 3      # hysteresis: require N wins to flip sticky label
IGNORE_HOSTS       = set()  # keep "???"; add "_gateway" here if you want to ignore it


def _paths(ip: str, settings: Dict) -> Dict[str, str]:
    """
    Compute artifact paths for the given target, honoring settings['paths']['traceroute'].
    Ensures the traceroute directory exists.
    """
    paths = resolve_all_paths(settings or {})
    tr_dir = paths.get("traceroute") or settings.get("traceroute_directory") or "traceroute"
    os.makedirs(tr_dir, exist_ok=True)
    stem = os.path.join(tr_dir, ip)
    return {
        "txt":   f"{stem}.trace.txt",
        "json":  f"{stem}.json",          # legacy hopN→host map
        "stats": f"{stem}_hops_stats.json",
        "hops":  f"{stem}_hops.json",     # list[{count, host}] with varies(...)
    }


def _load_stats(p: str) -> dict:
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_stats(p: str, stats: dict) -> None:
    with open(p, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)


def _write_hops_json(stats: dict, hops_path: str) -> None:
    """
    Convert rolling stats -> compact [{count, host}] labels file.
    Ignores any hop < 1 (filters out legacy hop0 if it exists on disk).

    Heuristic:
      - If the dominant host's share < UNSTABLE_THRESHOLD and there are >= 2 hosts,
        write "varies (a, b, c)" listing up to TOPK_TO_SHOW.
      - Else write the sticky 'last' host if present, otherwise the top host.
    """
    labels = []
    # Sort by hop index
    for hop_str, s in sorted(stats.items(), key=lambda kv: int(kv[0])):
        try:
            hop_int = int(hop_str)
        except Exception:
            continue
        if hop_int < 1:
            continue

        items = [(k, s[k]) for k in s if isinstance(s.get(k), int) and k not in IGNORE_HOSTS]
        total = sum(c for _, c in items)
        if total == 0:
            continue

        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total

        if share < UNSTABLE_THRESHOLD and len(items) >= 2:
            sample = ", ".join(h for h, _ in items[:TOPK_TO_SHOW])
            host_label = f"varies ({sample})"
        else:
            host_label = s.get("last") or top_host

        labels.append({"count": hop_int, "host": host_label})

    if labels:
        with open(hops_path, "w", encoding="utf-8") as f:
            json.dump(labels, f, indent=2)


def update_hop_labels_only(ip: str, hops: list, settings: dict, logger=None) -> None:
    """
    Re-write <ip>_hops.json from EXISTING stats ONLY.
    (No stat updates here → avoids double counting. Stats maintained elsewhere.)
    """
    p = _paths(ip, settings)
    stats = _load_stats(p["stats"])
    _write_hops_json(stats, p["hops"])
    if logger:
        logger.debug(f"[{ip}] Hop labels refreshed (no stat changes) -> {p['hops']}")


def save_trace_and_json(ip: str, hops: list, settings: dict, logger=None) -> None:
    """
    Save traceroute results for a target in two formats (plus 2 auxiliary files):

    1) <target>.trace.txt — text lines: "<hop> <ip/host> <avg> ms"
    2) <target>.json      — dict: {"hop1": "1.2.3.4", "hop2": "5.6.7.8", ...}

    Aux (maintained elsewhere):
    3) <target>_hops_stats.json — rolling counts per hop for label stabilization
    4) <target>_hops.json       — list[{count, host}] with stable or "varies(...)" labels
    """
    p = _paths(ip, settings)

    # 1) Plain text (human-readable), skipping hop0
    with open(p["txt"], "w", encoding="utf-8") as f:
        for hop in hops:
            try:
                hop_num = int(hop.get("count", 0))
            except (TypeError, ValueError):
                continue
            if hop_num < 1:
                continue
            ip_addr = hop.get("host", "?")
            latency = hop.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")
    if logger:
        logger.info(f"Saved traceroute to {p['txt']}")

    # 2) Simple hopN → host map (legacy/kept), skipping hop0
    hop_map = {}
    for hop in hops:
        try:
            hop_num = int(hop.get("count", 0))
        except (TypeError, ValueError):
            continue
        if hop_num < 1:
            continue
        hop_map[f"hop{hop_num}"] = hop.get("host", f"hop{hop_num}")
    with open(p["json"], "w", encoding="utf-8") as f:
        json.dump(hop_map, f, indent=2)
    if logger:
        logger.info(f"Saved hop label map to {p['json']}")

    # 3) Refresh stabilized labels from existing stats (no stat mutation here)
    update_hop_labels_only(ip, hops, settings, logger)
