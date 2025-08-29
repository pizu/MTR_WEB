#!/usr/bin/env python3
"""
modules/index_helpers.py
========================

Shared helper functions for the index page:
- HTML escaping
- Reading "last seen" from per-IP logs
- Reading hop count from traceroute JSON
- Classifying simple status from fping
- Building the card model for the Dashboard

All I/O paths must come from resolve_all_paths(settings) so this stays config-driven.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from modules.fping_status import get_fping_status


def html_escape(s: Any) -> str:
    """Minimal HTML escaping for safe text injection."""
    if s is None:
        return ""
    s = str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))


def read_last_seen_from_log(log_path: str) -> str:
    """
    Extract a human-readable 'Last Seen' timestamp from <ip>.log.
    Priority:
      1) Last 'MTR RUN' line → leading timestamp if present
      2) File mtime
      3) 'Never' / 'Unknown'
    """
    if not os.path.exists(log_path):
        return "Never"

    last_line = None
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "MTR RUN" in line:
                    last_line = line.strip()
    except Exception:
        last_line = None

    if last_line:
        parts = last_line.split(" [", 1)
        ts = parts[0].strip() if parts else ""
        if len(ts) >= 19 and ts[4] == "-" and ts[7] == "-" and ts[10] == " ":
            return ts
        return last_line

    try:
        return datetime.fromtimestamp(os.path.getmtime(log_path)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"


def read_hop_count(traceroute_dir: str, ip: str) -> Optional[int]:
    """
    Gets count of hop records from <traceroute>/<ip>_hops.json if present.
    """
    path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f) or []
            return len(arr)
    except Exception:
        pass
    return None


def classify_status_from_fping(raw: str) -> str:
    """
    Normalize fping output to one of: 'up' | 'down' | 'warn' | 'unknown'.
    (Currently: 'alive'→up, 'unreachable'→down; extend later for 'warn')
    """
    if not raw:
        return "unknown"
    r = raw.strip().lower()
    if r == "alive":
        return "up"
    if r == "unreachable":
        return "down"
    return "unknown"


def build_cards(targets: List[Dict[str, Any]], paths: Dict[str, str], enable_fping: bool, logger) -> List[Dict[str, str]]:
    """
    Create a list of dictionaries (cards) ready for templating the Dashboard.

    Each card includes:
      - ip, desc
      - status_class ('up'|'down'|'warn'|'unknown')
      - status_label (original FPING text uppercased; or 'UNKNOWN')
      - last_seen (from logs)
      - hops (count or '—')

    Returns
    -------
    list[dict]
    """
    cards = []
    log_dir    = paths["logs"]
    tracer_dir = paths["traceroute"]
    fping_bin  = paths.get("fping")

    for t in (targets or []):
        ip = (t or {}).get("ip") or ""
        if not ip:
            continue
        desc = (t or {}).get("description", "") or ""

        log_path  = os.path.join(log_dir, f"{ip}.log")
        last_seen = read_last_seen_from_log(log_path)

        status_raw = "Unknown"
        if enable_fping:
            try:
                status_raw = get_fping_status(ip, fping_bin)
            except Exception as e:
                logger.warning(f"fping status failed for {ip}: {e}")

        status_class = classify_status_from_fping(status_raw)
        hop_count    = read_hop_count(tracer_dir, ip)
        hop_text     = str(hop_count) if hop_count is not None else "—"

        cards.append({
            "ip": ip,
            "desc": desc,
            "status_class": status_class,
            "status_label": (status_raw or "Unknown").upper(),
            "last_seen": last_seen,
            "hops": hop_text,
        })

    return cards
