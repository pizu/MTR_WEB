#!/usr/bin/env python3
"""
modules/rrd_exporter.py

Exports Chart.js‑friendly JSON time series for a target IP and a given time range label.
Output path: <paths.html>/data/<ip>_<label>.json

Update (per-hop 'varies' flag):
- For each hop, compute a boolean "varies" that indicates the hop's endpoint (IP/host)
  has changed across exports.
- Colors remain derived from hop index, so a hop keeps the same color even if its IP changes.
- A tiny cache is kept on disk so that subsequent exports can detect changes over time.

This does NOT add any new plotted metrics and does NOT require RRD schema changes.
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
# Helpers
# -----------------------------

# Deterministic color by hop index (stable across exports)
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
        if isinstance(v, float) and (v != v):  # NaN
            return None
        return float(v)
    except Exception:
        return None

def _ensure_dir(p: str):
    os.makedirs(os.path.dirname(p), exist_ok=True)

# Extract the hop endpoint from a legend label like "7: 217.15.98.96"
# If the label format changes, tweak the regex.
LABEL_ENDPOINT_RE = re.compile(r"^\s*\d+\s*:\s*([^\s]+)")

def _extract_endpoint(label_text: str) -> str:
    m = LABEL_ENDPOINT_RE.match(label_text or "")
    return m.group(1) if m else (label_text or "")

# -------------- varies cache --------------

def _cache_dir(paths: Dict[str, str], html_dir: str) -> str:
    """
    Choose a writable cache dir:
    - Prefer paths['cache'] if defined by your utils
    - Else create <html_dir>/var/hop_ip_cache
    """
    base = (paths or {}).get("cache")
    if not base:
        base = os.path.join(html_dir, "var", "hop_ip_cache")
    os.makedirs(base, exist_ok=True)
    return base

def _cache_path(cache_dir: str, ip: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_.-]", "_", ip)
    return os.path.join(cache_dir, f"{safe}.hopips.json")

def _load_cache(cache_file: str) -> Dict[str, List[str]]:
    """
    Cache format:
      {
        "1": ["172.16.21.1","172.16.21.254"],
        "2": ["10.69.5.3"],
        ...
      }
    """
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        # normalize values to lists of strings
        out: Dict[str, List[str]] = {}
        for k, v in data.items():
            if isinstance(v, list):
                out[k] = [str(x) for x in v if x is not None]
        return out
    except Exception:
        return {}

def _save_cache(cache_file: str, data: Dict[str, List[str]]) -> None:
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        # caching is best-effort; ignore failures
        pass

def _update_cache_with_current(cache: Dict[str, List[str]],
                               hops_legend: List[Tuple[int, str]],
                               max_values_per_hop: int = 20) -> Dict[str, bool]:
    """
    Update cache with current endpoints; return a dict {hop_index_str: varies_bool}.
    'varies' is True if we have recorded >1 distinct endpoints for that hop.
    """
    varies_flags: Dict[str, bool] = {}
    for hop_index, label_text in hops_legend:
        key = str(int(hop_index))
        endpoint = _extract_endpoint(label_text)
        lst = cache.get(key, [])
        if endpoint and endpoint not in lst:
            lst.append(endpoint)
            # bound the list length
            if len(lst) > max_values_per_hop:
                lst = lst[-max_values_per_hop:]
            cache[key] = lst
        varies_flags[key] = len(lst) > 1
    return varies_flags

# -----------------------------
# Export
# -----------------------------

def export_ip_timerange_json(ip: str, settings: dict, label: str, seconds: int, logger=None) -> str:
    """
    Export one JSON bundle for (ip, label, seconds).
    Adds per-hop "varies": true/false (endpoint changed across exports).

    JSON structure (excerpt):

    {
      "ip": "8.8.8.8",
      "label": "30m",
      "seconds": 1800,
      "step": 60,
      "timestamps": [...],
      "epoch": [...],
      "hops": [
        {
          "hop": 1,
          "name": "1: 172.16.21.1",
          "color": "#a4dd0a",
          "varies": false,          <-- NEW (per-hop flag, not a metric)
          "metrics": {
            "avg": [...],
            "last": [...],
            "best": [...],
            "loss": [...]
          }
        },
        ...
      ]
    }
    """
    # === Directories (unified paths) ===
    paths = resolve_all_paths(settings)
    RRD_DIR  = paths["rrd"]
    HTML_DIR = resolve_html_dir(settings)  # ensures <paths.html> exists
    DATA_DIR = os.path.join(HTML_DIR, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
    _ensure_dir(os.path.join(DATA_DIR, "x"))  # ensure folder exists

    # Metrics to export (from DS schema)
    schema_metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]

    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")

    # If missing RRD, write stub
    if not os.path.exists(rrd_path):
        out = {
            "ip": ip, "label": label, "seconds": int(seconds), "step": None,
            "timestamps": [], "epoch": [], "hops": []
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        if logger:
            logger.warning(f"[{ip}] RRD missing, wrote stub: {out_path}")
        return out_path

    # Build legend labels (current snapshot labels per hop)
    traceroute_dir = paths["traceroute"]
    hops_legend = get_labels(ip, traceroute_dir=traceroute_dir) or []

    # --- per-hop varies detection with persistent cache ---
    cache_dir   = _cache_dir(paths, HTML_DIR)
    cache_file  = _cache_path(cache_dir, ip)
    cache_state = _load_cache(cache_file)                 # { "1": [ "172.16.21.1", ... ], ... }
    varies_map  = _update_cache_with_current(cache_state, hops_legend)  # { "1": bool, ... }
    _save_cache(cache_file, cache_state)

    # --- fetch data once (AVERAGE) for the time grid & series ---
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
            "timestamps": [], "epoch": [], "hops": []
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        return out_path

    step = int(f_step) if f_step else None
    epochs = list(range(int(f_start), int(f_end), step)) if step and step > 0 else list(range(len(rows)))

    if len(epochs) != len(rows):
        n = min(len(epochs), len(rows))
        epochs = epochs[:n]
        rows = rows[:n]

    labels_hhmm = [_fmt_ts(ts if step else int(time.time())) for ts in epochs]

    # Map DS names to column indices
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

    # Build hop entries with per‑metric arrays + per‑hop varies flag
    hop_entries = []
    for hop_index, label_text in hops_legend:
        entry = {
            "hop": int(hop_index),
            "name": str(label_text),
            "color": _color(int(hop_index)),            # color stable by hop index
            "varies": bool(varies_map.get(str(hop_index), False)),  # <-- NEW
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
