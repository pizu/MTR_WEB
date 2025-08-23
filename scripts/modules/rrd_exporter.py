#!/usr/bin/env python3
"""
modules/rrd_exporter.py

Purpose
-------
Export Chart.js‑friendly JSON time‑series for one target IP and one time range label
(e.g. "30m", "1h"). The output is written under <html_dir>/data/<ip>_<label>.json.

What this script writes
-----------------------
Per bundle (file):
  - ip, label, seconds, step, timestamps[], epoch[]
  - rrd_window: {start_epoch, end_epoch, step}           # <-- NEW
  - hops[]:
      {
        hop: <int>,
        name: "<hop>: <endpoint>",
        color: "#rrggbb",                                # stable color per hop index
        varies: <bool>,                                  # true if hop used >1 endpoint in history
        endpoints: ["ip1", "ip2", ...],                  # all distinct endpoints ever seen
        changes: [                                       # full history (epoch seconds)
          {"ip":"ip1","first":1693500000,"last":1693502400},
          {"ip":"ip2","first":1693503000,"last":1693504200}
        ],
        changes_in_window: [ ... ],                      # <-- NEW: history intersected with this export window
        metrics: { "avg":[...], "last":[...], "best":[...], "loss":[...] }
      }

Design notes
------------
- RRD persists numeric series only (avg/last/best/loss...). The "varies" concept is derived
  from hop labels (traceroute), so we keep a small JSON cache with per-hop endpoint history.
- We expose the RRD window in every JSON bundle so any calendar/timeline you build later
  can be grounded in the *actual* span we fetched from RRD.
- No RRD schema changes are required.

Suitable for users with basic Python knowledge:
- Functions are short and documented.
- Only standard library + rrdtool is used.

"""

import os
import re
import math
import time
import json
import rrdtool
from datetime import datetime
from typing import Dict, List, Tuple, Any

from modules.graph_utils import get_labels
from modules.utils import resolve_html_dir, resolve_all_paths

# -----------------------------
# Small helpers
# -----------------------------

def _now_epoch() -> int:
    return int(time.time())

def _color(hop_index: int) -> str:
    """Deterministic color based on hop index (keeps color stable even if IP changes)."""
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"#{r:02x}{g:02x}{b:02x}"

def _fmt_ts(epoch: int) -> str:
    try:
        return datetime.fromtimestamp(epoch).strftime("%H:%M")
    except Exception:
        return ""

def _nan_to_none(v):
    """Convert NaN/None to None, otherwise return float(v)."""
    try:
        if v is None:
            return None
        if isinstance(v, float) and (v != v):  # NaN
            return None
        return float(v)
    except Exception:
        return None

def _ensure_dir(p: str):
    os.makedirs(os.path.dirname(p), exist_ok=True)

# Extract endpoint from a legend label like "7: 217.15.98.96".
LABEL_ENDPOINT_RE = re.compile(r"^\s*\d+\s*:\s*([^\s]+)")

def _extract_endpoint(label_text: str) -> str:
    m = LABEL_ENDPOINT_RE.match(label_text or "")
    return m.group(1) if m else (label_text or "")

# -----------------------------
# Hop-IP cache with timestamps
# -----------------------------

def _cache_dir(paths: Dict[str, str], html_dir: str) -> str:
    """
    Pick a writable cache directory:
      - Prefer paths['cache'] when provided by your utils.
      - Else use <html_dir>/var/hop_ip_cache
    """
    base = (paths or {}).get("cache")
    if not base:
        base = os.path.join(html_dir, "var", "hop_ip_cache")
    os.makedirs(base, exist_ok=True)
    return base

def _cache_path(cache_dir: str, ip: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_.-]", "_", ip)
    return os.path.join(cache_dir, f"{safe}.hopips.json")

def _normalize_cache_entry_list(raw_val) -> List[Dict[str, Any]]:
    """
    Accept either:
      - list[str]                                      (legacy format)
      - list[{"ip":str,"first":int,"last":int}]        (current format)
    Returns a list of dicts with keys ip/first/last (deduped by ip).
    """
    out: List[Dict[str, Any]] = []
    now = _now_epoch()
    if not isinstance(raw_val, list):
        return out
    for v in raw_val:
        if isinstance(v, dict) and "ip" in v:
            ip = str(v.get("ip") or "")
            if not ip:
                continue
            first = int(v.get("first") or now)
            last  = int(v.get("last")  or first)
            out.append({"ip": ip, "first": first, "last": last})
        else:
            ip = str(v or "")
            if not ip:
                continue
            out.append({"ip": ip, "first": now, "last": now})
    # merge duplicates by IP
    merged: Dict[str, Dict[str, Any]] = {}
    for rec in out:
        ip = rec["ip"]
        prev = merged.get(ip)
        if prev:
            prev["first"] = min(prev["first"], rec["first"])
            prev["last"]  = max(prev["last"],  rec["last"])
        else:
            merged[ip] = dict(rec)
    return list(merged.values())

def _load_cache(cache_file: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load cache JSON (per-hop lists), tolerant to legacy format."""
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        out: Dict[str, List[Dict[str, Any]]] = {}
        for k, v in data.items():
            out[k] = _normalize_cache_entry_list(v)
        return out
    except Exception:
        return {}

def _save_cache(cache_file: str, data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Best-effort write of the cache file."""
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

def _update_cache_with_current(
    cache: Dict[str, List[Dict[str, Any]]],
    hops_legend: List[Tuple[int, str]],
    max_values_per_hop: int = 20
) -> Dict[str, bool]:
    """
    Update cache with the current endpoints:
      - If endpoint already present for hop -> update its 'last' timestamp.
      - Else append a new record with first=last=now (and cap list length).
    Returns {hop_index_str: varies_bool} where varies means >1 distinct IPs.
    """
    now = _now_epoch()
    varies_flags: Dict[str, bool] = {}
    for hop_index, label_text in hops_legend:
        key = str(int(hop_index))
        endpoint = _extract_endpoint(label_text)
        lst = cache.get(key, [])
        found = False
        for rec in lst:
            if rec.get("ip") == endpoint:
                rec["last"] = now
                found = True
                break
        if not found and endpoint:
            lst.append({"ip": endpoint, "first": now, "last": now})
            # keep only the newest N by 'first'
            lst = sorted(lst, key=lambda r: int(r.get("first", 0)))  # oldest..newest
            if len(lst) > max_values_per_hop:
                lst = lst[-max_values_per_hop:]
        cache[key] = lst
        varies_flags[key] = len({rec.get("ip") for rec in lst if rec.get("ip")}) > 1
    return varies_flags

def _clip_changes_to_window(changes: List[Dict[str, Any]], start_epoch: int, end_epoch: int) -> List[Dict[str, Any]]:
    """
    Return copies of change records that overlap the [start_epoch, end_epoch) window.
    Times are clipped to the window so your UI/calendar can use them directly.
    """
    if not isinstance(changes, list):
        return []
    out: List[Dict[str, Any]] = []
    for rec in changes:
        try:
            ip = rec.get("ip")
            a = int(rec.get("first", 0))
            b = int(rec.get("last", a))
            # no overlap
            if b < start_epoch or a >= end_epoch:
                continue
            a2 = max(a, start_epoch)
            b2 = min(b, end_epoch)
            out.append({"ip": str(ip), "first": a2, "last": b2})
        except Exception:
            continue
    return out

# -----------------------------
# Export
# -----------------------------

def export_ip_timerange_json(ip: str, settings: dict, label: str, seconds: int, logger=None) -> str:
    """
    Export one JSON bundle for (ip, label, seconds).
    Includes:
      - per-hop metrics
      - per-hop variation info (varies, endpoints, changes)
      - per-hop changes_in_window (history clipped to RRD window)
      - top-level rrd_window {start_epoch, end_epoch, step}
    """
    paths = resolve_all_paths(settings)
    RRD_DIR  = paths["rrd"]
    HTML_DIR = resolve_html_dir(settings)
    DATA_DIR = os.path.join(HTML_DIR, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
    _ensure_dir(os.path.join(DATA_DIR, "x"))

    # Which metrics to export (from DS schema in settings)
    schema_metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]

    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")

    # If missing RRD, write a stub
    if not os.path.exists(rrd_path):
        out = {
            "ip": ip, "label": label, "seconds": int(seconds), "step": None,
            "timestamps": [], "epoch": [], "rrd_window": None, "hops": []
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        if logger:
            logger.warning(f"[{ip}] RRD missing, wrote stub: {out_path}")
        return out_path

    # Legend labels (current snapshot)
    traceroute_dir = paths["traceroute"]
    hops_legend = get_labels(ip, traceroute_dir=traceroute_dir) or []

    # Cache (history) update
    cache_dir   = _cache_dir(paths, HTML_DIR)
    cache_file  = _cache_path(cache_dir, ip)
    cache_state = _load_cache(cache_file)                              # { "1": [ {ip,first,last}, ... ], ... }
    varies_map  = _update_cache_with_current(cache_state, hops_legend)
    _save_cache(cache_file, cache_state)

    # Fetch RRD: build time grid & series
    end = int(time.time())
    start = end - int(seconds)
    try:
        (f_start, f_end, f_step), names, rows = rrdtool.fetch(
            rrd_path, "AVERAGE", "--start", str(start), "--end", str(end)
        )
    except rrdtool.OperationalError as e:
        if logger:
            logger.warning(f"[{ip}] fetch failed for {label}: {e}")
        out = {
            "ip": ip, "label": label, "seconds": int(seconds), "step": None,
            "timestamps": [], "epoch": [], "rrd_window": None, "hops": []
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        return out_path

    step = int(f_step) if f_step else None
    epochs = list(range(int(f_start), int(f_end), step)) if step and step > 0 else list(range(len(rows)))

    # Keep arrays aligned
    if len(epochs) != len(rows):
        n = min(len(epochs), len(rows))
        epochs = epochs[:n]
        rows = rows[:n]

    labels_hhmm = [_fmt_ts(ts if step else int(time.now())) for ts in epochs]

    # Map DS names to columns
    name_to_idx = {name: i for i, name in enumerate(names or [])}

    def extract_series(ds_name: str):
        col = name_to_idx.get(ds_name, None)
        if col is None:
            return [None] * len(rows)
        out_vals = []
        for r in rows:
            try:
                val = r[col]
            except Exception:
                val = None
            out_vals.append(_nan_to_none(val))
        return out_vals

    # Build hops
    hop_entries = []
    window_start = int(f_start)
    window_end   = int(f_end)
    for hop_index, label_text in hops_legend:
        key = str(int(hop_index))
        full_changes = cache_state.get(key, [])  # list of {ip,first,last}
        clipped      = _clip_changes_to_window(full_changes, window_start, window_end)
        entry = {
            "hop": int(hop_index),
            "name": str(label_text),
            "color": _color(int(hop_index)),
            "varies": bool(varies_map.get(key, False)),
            "endpoints": [rec["ip"] for rec in full_changes],
            "changes": full_changes,
            "changes_in_window": clipped,  # <-- for UI/calendar to use immediately
            "metrics": {}
        }
        for m_schema in schema_metrics:
            ds = f"hop{hop_index}_{m_schema}"
            entry["metrics"][m_schema] = extract_series(ds)
        hop_entries.append(entry)

    out = {
        "ip": ip,
        "label": label,
        "seconds": int(seconds),
        "step": step,
        "timestamps": labels_hhmm,
        "epoch": epochs,
        "rrd_window": { "start_epoch": window_start, "end_epoch": window_end, "step": step },
        "hops": hop_entries
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    if logger:
        changed = [h for h in hop_entries if h.get("varies")]
        if changed:
            logger.info(f"[{ip}] varies on hops: {', '.join(str(h['hop']) for h in changed)}")
        logger.info(f"[{ip}] exported {out_path} ({len(epochs)} points, hops={len(hop_entries)})")
    return out_path
