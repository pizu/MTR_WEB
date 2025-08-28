#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/graph_utils.py (single-writer architecture)

Responsibilities
---------------
- Maintain per-hop stats for labels (varies detection).
- Persist sanitized hop stats JSON (no bookkeeping keys).
- Build and persist hop labels JSON.
- Optionally persist full trace JSON when asked.

Inputs
------
- ip (str): target address
- hops (list[dict]): MTR JSON snapshot rows, each like:
    {
        "count": <int hop index (1-based)>,
        "host":  <str Host column value: IP/DNS/'???' or empty>
        ... plus MTR metrics ...
    }
- settings (dict): uses settings['paths']['traceroute'] and settings['labels']:
    reset_mode, unstable_threshold, topk_to_show, majority_window, sticky_min_wins

Public API
----------
update_labels_and_traces(ip, hops, settings, write_trace_json=False, prev_hops=None, logger=None)
    → Updates stats & labels; if write_trace_json=True also writes a simple trace JSON.

Implementation notes
--------------------
- Host tokens mirror MTR's Host column exactly. We keep literal '???'.
- Empty / '*'/ '?' Hosts are normalized to '(waiting for reply)'.
- Bookkeeping keys ('_order', 'last', 'wins') are *never* written to disk.
- We sanitize both on load and on save to prevent legacy pollution.
"""

from __future__ import annotations

import os
import json
from typing import Dict, List, Tuple, Optional

# --------------------------------------------------------------------
# Config / constants
# --------------------------------------------------------------------
RESERVED_KEYS = {"_order", "last", "wins"}
WAITING = "(waiting for reply)"  # used when host is empty/None/'*'/'?'


# --------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------
def _tr_dir(settings: dict) -> str:
    """Return traceroute directory from settings (must exist in YAML)."""
    paths = (settings or {}).get("paths") or {}
    tr_dir = paths.get("traceroute")
    if not tr_dir:
        raise RuntimeError("settings.paths.traceroute is required")
    os.makedirs(tr_dir, exist_ok=True)
    return tr_dir


def _paths(ip: str, settings: dict) -> Tuple[str, str, str]:
    """Return (stats_path, hops_json_path, trace_json_path) for target."""
    base = os.path.join(_tr_dir(settings), ip)
    return base + "_hops_stats.json", base + "_hops.json", base + "_trace.json"


def normalize_host_label(host) -> str:
    """Mirror the MTR Host column tokens; keep '???' verbatim."""
    if host is None:
        return WAITING
    h = str(host).strip()
    if not h or h in {"*", "?"}:
        return WAITING
    return h  # includes "???", IPs, DNS names unchanged


def _strip_reserved(d: dict) -> dict:
    """Remove bookkeeping keys from each hop bucket."""
    if not isinstance(d, dict):
        return {}
    cleaned = {}
    for hop, data in d.items():
        if isinstance(data, dict):
            cleaned[hop] = {k: v for k, v in data.items() if k not in RESERVED_KEYS}
    return cleaned


def _load_stats(stats_path: str) -> dict:
    """Load stats from disk and sanitize (defensive for legacy files)."""
    try:
        with open(stats_path, encoding="utf-8") as f:
            raw = json.load(f) or {}
    except Exception:
        return {}
    return _strip_reserved(raw)


def _save_stats(stats_path: str, stats: dict) -> None:
    """Persist only sanitized stats."""
    os.makedirs(os.path.dirname(stats_path), exist_ok=True)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(_strip_reserved(stats or {}), f, indent=2)


# --------------------------------------------------------------------
# Stats maintenance
# --------------------------------------------------------------------
def _label_knobs(settings: dict) -> dict:
    """Extract label knobs strictly from YAML."""
    labels = (settings or {}).get("labels") or {}
    return {
        "reset_mode":         str(labels["reset_mode"]).strip().lower(),
        "unstable_threshold": float(labels["unstable_threshold"]),
        "topk_to_show":       int(labels["topk_to_show"]),
        "majority_window":    int(labels["majority_window"]),
        "sticky_min_wins":    int(labels["sticky_min_wins"]),
    }


def _first_diff_index(prev_hops: List[dict], curr_hops: List[dict]) -> Optional[int]:
    """Return the first hop index (1-based) where host tokens differ; None if identical length/content."""
    n = min(len(prev_hops), len(curr_hops))
    for i in range(n):
        if (prev_hops[i] or {}).get("host") != (curr_hops[i] or {}).get("host"):
            return i + 1
    if len(prev_hops) != len(curr_hops):
        return n + 1
    return None


def _reset_stats_from(stats: dict, start_hop_int: int) -> None:
    """Remove stats buckets for hop indices >= start_hop_int (inclusive)."""
    for k in [k for k in stats.keys() if k.isdigit() and int(k) >= start_hop_int]:
        stats.pop(k, None)


def _realign_then_reset(stats: dict, curr_hops: List[dict]) -> None:
    """Produce an empty skeleton aligned to current hops (minimal carry-over)."""
    new_stats = {}
    for h in curr_hops:
        try:
            idx = int(h.get("count", 0))
        except (TypeError, ValueError):
            continue
        if idx < 1:
            continue
        new_stats[str(idx)] = {"_order": [], "last": None, "wins": 0}
    stats.clear()
    stats.update(new_stats)


def _apply_reset_policy(stats: dict, prev_hops: List[dict], curr_hops: List[dict], reset_mode: str) -> None:
    """Apply the configured reset policy when a path change is detected."""
    fd = _first_diff_index(prev_hops, curr_hops)
    if fd is None:
        return
    if reset_mode == "none":
        return
    if reset_mode == "all":
        stats.clear()
        return
    if reset_mode == "realign_then_reset":
        _realign_then_reset(stats, curr_hops)
        return
    _reset_stats_from(stats, fd)  # default: from_first_diff


def _update_stats_with_snapshot(stats: dict, hops: List[dict],
                                majority_window: int, sticky_min_wins: int) -> dict:
    """Update rolling stats for each hop bucket."""
    for h in hops:
        try:
            hop_num = int(h.get("count", 0))
        except (TypeError, ValueError):
            continue
        if hop_num < 1:
            continue

        hop_idx = str(hop_num)
        host = normalize_host_label(h.get("host"))
        s = stats.setdefault(hop_idx, {"_order": [], "last": None, "wins": 0})

        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)
        s[host] += 1

        total_counts = sum(v for k, v in s.items() if isinstance(s.get(k), int))
        if total_counts > majority_window:
            for key in list(s["_order"])[::-1]:
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

        # Sticky modal excluding RESERVED_KEYS
        modal = max(
            (k for k in s if isinstance(s.get(k), int) and k not in RESERVED_KEYS),
            key=lambda k: s[k],
            default=None
        )
        cur = s.get("last")
        if cur is None:
            s["last"] = modal
            s["wins"] = 1
        elif modal == cur:
            s["wins"] = min(s.get("wins", 0) + 1, sticky_min_wins)
        else:
            s["wins"] = s.get("wins", 0) - 1
            if s["wins"] <= 0:
                s["last"] = modal
                s["wins"] = 1

    return stats


# --------------------------------------------------------------------
# Label building & persistence
# --------------------------------------------------------------------
def _decide_labels_and_write(stats: dict, hops_json_path: str,
                             unstable_threshold: float, topk_to_show: int) -> Dict[int, str]:
    """
    Build per-hop labels and persist <ip>_hops.json.
    Returns {hop_index: "N: label"} for convenience.
    """
    labels: Dict[int, str] = {}
    out: List[dict] = []

    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        hop_int = int(hop_str)
        if hop_int < 1:
            continue

        # Consider only real host tokens (remove bookkeeping keys)
        items = [(k, s[k]) for k in s
                 if isinstance(s.get(k), int) and k not in RESERVED_KEYS]
        total = sum(c for _, c in items)
        if total == 0:
            continue

        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total

        if share < unstable_threshold and len(items) >= 2:
            sample_hosts = []
            for token, _cnt in items[:topk_to_show]:
                if token not in RESERVED_KEYS and token not in sample_hosts:
                    sample_hosts.append(token)  # keep '???' as-is
            host_label = f"varies ({', '.join(sample_hosts)})" if sample_hosts else "varies"
        else:
            last_host = (s.get("last") if s.get("last") not in RESERVED_KEYS else None)
            host_label = last_host or top_host

        labels[hop_int] = f"{hop_int}: {host_label}"
        out.append({"count": hop_int, "host": host_label})

    if out:
        with open(hops_json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)

    return labels


# --------------------------------------------------------------------
# Public API (single writer)
# --------------------------------------------------------------------
def update_labels_and_traces(
    ip: str,
    hops: List[dict],
    settings: dict,
    write_trace_json: bool = False,
    prev_hops: Optional[List[dict]] = None,
    logger=None
) -> Dict[int, str]:
    """
    Single entrypoint called by monitor.py.

    Steps:
      1) Load existing stats (sanitized).
      2) Optionally apply reset policy if the path changed.
      3) Update per-hop stats with the new snapshot.
      4) Save sanitized stats to disk.
      5) Build labels from sanitized stats and write <ip>_hops.json.
      6) Optionally write <ip>_trace.json with the raw snapshot.

    Returns:
      dict[int, str]: {hop_index: "N: label"}
    """
    stats_path, hops_json_path, trace_json_path = _paths(ip, settings)
    knobs = _label_knobs(settings)

    # (1) Load
    stats = _load_stats(stats_path)

    # (2) Reset policy
    if prev_hops is not None:
        _apply_reset_policy(stats, prev_hops, hops, knobs["reset_mode"])

    # (3) Update rolling stats
    stats = _update_stats_with_snapshot(
        stats, hops,
        majority_window=knobs["majority_window"],
        sticky_min_wins=knobs["sticky_min_wins"]
    )

    # (4) Save sanitized stats
    _save_stats(stats_path, stats)

    # (5) Build labels (from sanitized on-disk shape) and write hops.json
    clean_stats = _load_stats(stats_path)  # belt-and-braces
    labels = _decide_labels_and_write(
        clean_stats,
        hops_json_path,
        unstable_threshold=knobs["unstable_threshold"],
        topk_to_show=knobs["topk_to_show"]
    )

    # (6) Optional: write full trace JSON
    if write_trace_json:
        trace_doc = {"ip": ip, "hops": hops}
        with open(trace_json_path, "w", encoding="utf-8") as f:
            json.dump(trace_doc, f, indent=2)

    if logger:
        logger.debug(f"[{ip}] graph_utils: stats+labels updated (write_trace_json={write_trace_json})")
    return labels
