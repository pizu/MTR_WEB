#!/usr/bin/env python3
"""
modules/rrd_exporter.py

Exports Chart.js‑friendly JSON time‑series for a target IP and a time range label.
Writes to <html_dir>/data/<ip>_<label>.json

Fields included:
- ip, label, seconds, step, timestamps[], epoch[]
- rrd_window: {start_epoch, end_epoch, step}
- hops[]:
    hop, name, color,
    varies (bool),
    endpoints [ip...],
    changes [{ip,first,last}...],
    changes_in_window [{ip,first,last}...]  # clipped to this bundle’s RRD window
    metrics {avg[], last[], best[], loss[], ...}

Notes:
- RRD stores only numeric DS; varies is derived from hop labels + a small cache.
- This version strictly follows YAML/env for traceroute folder, logging the choice.
"""

import os
import re
import math
import time
import json
import rrdtool
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

from modules.graph_utils import get_labels
from modules.utils import resolve_html_dir, resolve_all_paths, setup_logger

# ---------- helpers ----------

def _now_epoch() -> int:
    return int(time.time())

def _color(hop_index: int) -> str:
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
    try:
        if v is None:
            return None
        if isinstance(v, float) and (v != v):
            return None
        return float(v)
    except Exception:
        return None

def _ensure_dir(p: str):
    os.makedirs(os.path.dirname(p), exist_ok=True)

LABEL_ENDPOINT_RE = re.compile(r"^\s*\d+\s*:\s*([^\s]+)")
def _extract_endpoint(label_text: str) -> str:
    m = LABEL_ENDPOINT_RE.match(label_text or "")
    return m.group(1) if m else (label_text or "")

# ---------- traceroute dir resolver (safeguard at exporter level) ----------

def _resolve_traceroute_dir(paths: Dict[str, str], settings: dict, logger=None) -> Optional[str]:
    """
    Preference order:
      1) env MTR_TRACEROUTE_DIR (if exists)
      2) settings.paths.traceroute (if exists)
      3) settings.paths.traces     (legacy key; if exists)
      4) paths['traceroute']       (from resolve_all_paths)
      5) /opt/scripts/MTR_WEB/traceroute (if exists)
      6) /opt/scripts/MTR_WEB/traces     (if exists)
    """
    cfg_paths = (settings or {}).get("paths", {}) or {}
    env_dir   = os.environ.get("MTR_TRACEROUTE_DIR")
    cfg_tr    = cfg_paths.get("traceroute")
    cfg_legacy= cfg_paths.get("traces")
    paths_tr  = (paths or {}).get("traceroute")

    candidates = []
    if env_dir:        candidates.append(("env:MTR_TRACEROUTE_DIR", env_dir))
    if cfg_tr:         candidates.append(("settings.paths.traceroute", cfg_tr))
    if cfg_legacy:     candidates.append(("settings.paths.traces", cfg_legacy))
    if paths_tr:       candidates.append(("paths.traceroute", paths_tr))
    candidates.extend([
        ("default:traceroute", "/opt/scripts/MTR_WEB/traceroute"),
        ("default:traces",     "/opt/scripts/MTR_WEB/traces"),
    ])

    chosen = None
    chosen_tag = None
    for tag, d in candidates:
        if d and os.path.isdir(d):
            chosen, chosen_tag = d, tag
            break

    if logger:
        if chosen:
            logger.info(f"Using traceroute dir ({chosen_tag}): {chosen}")
        else:
            logger.warning("No usable traceroute path found; hop labels empty and 'varies' cannot update.")
    return chosen

# ---------- hop‑IP cache with timestamps ----------

def _cache_dir(paths: Dict[str, str], html_dir: str) -> str:
    base = (paths or {}).get("cache")
    if not base:
        base = os.path.join(html_dir, "var", "hop_ip_cache")
    os.makedirs(base, exist_ok=True)
    return base

def _cache_path(cache_dir: str, ip: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_.-]", "_", ip)
    return os.path.join(cache_dir, f"{safe}.hopips.json")

def _normalize_cache_entry_list(raw_val) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now = _now_epoch()
    if not isinstance(raw_val, list):
        return out
    for v in raw_val:
        if isinstance(v, dict) and "ip" in v:
            ip = str(v.get("ip") or "")
            if not ip: continue
            first = int(v.get("first") or now)
            last  = int(v.get("last")  or first)
            out.append({"ip": ip, "first": first, "last": last})
        else:
            ip = str(v or "")
            if not ip: continue
            out.append({"ip": ip, "first": now, "last": now})
    # merge duplicates
    merged: Dict[str, Dict[str, Any]] = {}
    for rec in out:
        ip = rec["ip"]
        if ip in merged:
            merged[ip]["first"] = min(merged[ip]["first"], rec["first"])
            merged[ip]["last"]  = max(merged[ip]["last"],  rec["last"])
        else:
            merged[ip] = dict(rec)
    return list(merged.values())

def _load_cache(cache_file: str) -> Dict[str, List[Dict[str, Any]]]:
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        return {k: _normalize_cache_entry_list(v) for k, v in data.items()}
    except Exception:
        return {}

def _save_cache(cache_file: str, data: Dict[str, List[Dict[str, Any]]]) -> None:
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
            lst = sorted(lst, key=lambda r: int(r.get("first", 0)))
            if len(lst) > max_values_per_hop:
                lst = lst[-max_values_per_hop:]
        cache[key] = lst
        varies_flags[key] = len({rec.get("ip") for rec in lst if rec.get("ip")}) > 1
    return varies_flags

def _clip_changes_to_window(changes: List[Dict[str, Any]], start_epoch: int, end_epoch: int) -> List[Dict[str, Any]]:
    if not isinstance(changes, list):
        return []
    out: List[Dict[str, Any]] = []
    for rec in changes:
        try:
            ip = rec.get("ip")
            a = int(rec.get("first", 0))
            b = int(rec.get("last", a))
            if b < start_epoch or a >= end_epoch:
                continue
            a2 = max(a, start_epoch)
            b2 = min(b, end_epoch)
            out.append({"ip": str(ip), "first": a2, "last": b2})
        except Exception:
            continue
    return out

# ---------- export ----------

def export_ip_timerange_json(ip: str, settings: dict, label: str, seconds: int, logger=None) -> str:
    logger = logger or setup_logger("rrd_exporter", settings=settings)

    paths = resolve_all_paths(settings)
    RRD_DIR  = paths["rrd"]
    HTML_DIR = resolve_html_dir(settings)
    DATA_DIR = os.path.join(HTML_DIR, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
    _ensure_dir(os.path.join(DATA_DIR, "x"))

    # DS names from YAML
    schema_metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]

    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")

    if not os.path.exists(rrd_path):
        out = {"ip": ip, "label": label, "seconds": int(seconds), "step": None,
               "timestamps": [], "epoch": [], "rrd_window": None, "hops": []}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        logger.warning(f"[{ip}] RRD missing, wrote stub: {out_path}")
        return out_path

    # Resolve traceroute dir strictly following YAML/ENV (with safe fallbacks)
    traceroute_dir = _resolve_traceroute_dir(paths, settings, logger=logger)

    # Current labels (may be empty if dir not found)
    hops_legend = get_labels(ip, traceroute_dir=traceroute_dir) or []

    # Update hop‑IP cache
    cache_dir   = _cache_dir(paths, HTML_DIR)
    cache_file  = _cache_path(cache_dir, ip)
    cache_state = _load_cache(cache_file)                 # { "1": [ {ip,first,last}, ... ], ... }
    varies_map  = _update_cache_with_current(cache_state, hops_legend)
    _save_cache(cache_file, cache_state)

    # Fetch RRD data
    end = int(time.time())
    start = end - int(seconds)
    try:
        (f_start, f_end, f_step), names, rows = rrdtool.fetch(
            rrd_path, "AVERAGE", "--start", str(start), "--end", str(end)
        )
    except rrdtool.OperationalError as e:
        logger.warning(f"[{ip}] fetch failed for {label}: {e}")
        out = {"ip": ip, "label": label, "seconds": int(seconds), "step": None,
               "timestamps": [], "epoch": [], "rrd_window": None, "hops": []}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        return out_path

    step = int(f_step) if f_step else None
    epochs = list(range(int(f_start), int(f_end), step)) if step and step > 0 else list(range(len(rows)))
    if len(epochs) != len(rows):
        n = min(len(epochs), len(rows))
        epochs, rows = epochs[:n], rows[:n]

    labels_hhmm = [_fmt_ts(ts) for ts in epochs]

    name_to_idx = {name: i for i, name in enumerate(names or [])}
    def extract_series(ds_name: str):
        col = name_to_idx.get(ds_name)
        if col is None:
            return [None] * len(rows)
        vals = []
        for r in rows:
            try:
                vals.append(_nan_to_none(r[col]))
            except Exception:
                vals.append(None)
        return vals

    # Build hops block
    hop_entries = []
    window_start = int(f_start)
    window_end   = int(f_end)
    for hop_index, label_text in hops_legend:
        key = str(int(hop_index))
        full_changes = cache_state.get(key, [])
        clipped      = _clip_changes_to_window(full_changes, window_start, window_end)
        entry = {
            "hop": int(hop_index),
            "name": str(label_text),
            "color": _color(int(hop_index)),
            "varies": bool(varies_map.get(key, False)),
            "endpoints": [rec["ip"] for rec in full_changes],
            "changes": full_changes,
            "changes_in_window": clipped,
            "metrics": {}
        }
        for m in schema_metrics:
            entry["metrics"][m] = extract_series(f"hop{hop_index}_{m}")
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

    changed = [h for h in hop_entries if h.get("varies")]
    if changed:
        logger.info(f"[{ip}] varies on hops: {', '.join(str(h['hop']) for h in changed)}")
    logger.info(f"[{ip}] exported {out_path} ({len(epochs)} points, hops={len(hop_entries)})")
    return out_path
