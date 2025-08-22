#!/usr/bin/env python3
"""
modules/rrd_exporter.py

Exports Chart.js‑friendly JSON time series for a target IP and a given time range label.
Output path: <paths.html>/data/<ip>_<label>.json

Updates in this version:
- Unified paths (RRD/HTML/Traceroute) via modules.utils.
- Derives DS names from settings['rrd']['data_sources'].
- **Maps DS name 'stdev' → JSON key 'varies'** so front-end shows “Varies (ms)”.
- If a DS is missing in the RRD (e.g., old schema), exporter emits an aligned array of nulls.
"""

import os
import math
import time
import json
import rrdtool
from datetime import datetime

from modules.graph_utils import get_labels
from modules.utils import resolve_html_dir, resolve_all_paths

# Alias map: RRD DS name -> JSON key
# This lets us present friendlier names without changing RRD DS schema.
JSON_KEY_ALIAS = {
    "stdev": "varies",   # show as "varies" in JSON/UI
}

def _color(hop_index: int) -> str:
    """Deterministic color per hop (matches graph_workers)."""
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"#{r:02x}{g:02x}{b:02x}"

def _fmt_ts(epoch: int) -> str:
    """Human‑friendly HH:MM timestamps for the x‑axis."""
    try:
        return datetime.fromtimestamp(epoch).strftime("%H:%M")
    except Exception:
        return ""

def _nan_to_none(v):
    """Convert NaN/None/non‑numeric to None for JSON."""
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

def export_ip_timerange_json(ip: str, settings: dict, label: str, seconds: int, logger=None) -> str:
    """
    Export a single JSON bundle for (ip, label, seconds).
    Returns the output file path.
    """
    # === Directories (unified paths) ===
    paths = resolve_all_paths(settings)
    RRD_DIR  = paths["rrd"]
    HTML_DIR = resolve_html_dir(settings)  # ensures <paths.html> exists
    DATA_DIR = os.path.join(HTML_DIR, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
    _ensure_dir(os.path.join(DATA_DIR, "x"))  # ensure folder exists

    # Metrics to export (names from schema, e.g., ["avg", "last", "best", "loss", "stdev"])
    schema_metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]
    # JSON keys we will actually emit (with aliasing)
    json_metrics = [JSON_KEY_ALIAS.get(m, m) for m in schema_metrics]

    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")

    # If missing RRD, write empty stub so UI can still load
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

    # Build legend labels from traceroute cache (unified path)
    traceroute_dir = paths["traceroute"]
    hops_legend = get_labels(ip, traceroute_dir=traceroute_dir) or []

    # Fetch once for the time grid + all DS columns available in the chosen RRA
    end = int(time.time())
    start = end - int(seconds)

    try:
        # returns ((start, end, step), names, rows)
        (f_start, f_end, f_step), names, rows = rrdtool.fetch(
            rrd_path, "AVERAGE", "--start", str(start), "--end", str(end)
        )
    except rrdtool.OperationalError as e:
        # If fetch fails entirely, write empty stub
        if logger:
            logger.warning(f"[{ip}] fetch failed for {label}: {e}")
        out = {
            "ip": ip, "label": label, "seconds": int(seconds), "step": None,
            "timestamps": [], "epoch": [], "hops": []
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        return out_path

    # Time grid
    step = int(f_step) if f_step else None
    if step and step > 0:
        epochs = list(range(int(f_start), int(f_end), step))
    else:
        epochs = list(range(len(rows)))

    # Align lengths defensively
    if len(epochs) != len(rows):
        n = min(len(epochs), len(rows))
        epochs = epochs[:n]
        rows = rows[:n]

    labels_hhmm = [_fmt_ts(ts if step else int(time.time())) for ts in epochs]

    # Map DS names to column indices
    name_to_idx = {name: i for i, name in enumerate(names or [])}

    # If no traceroute labels (rare), infer hop indices from DS names (e.g., hop1_avg)
    if not hops_legend and names:
        seen_hops = set()
        for nm in names:
            parts = nm.split("_", 1)
            if len(parts) == 2 and parts[0].startswith("hop"):
                try:
                    idx = int(parts[0][3:])
                    seen_hops.add(idx)
                except Exception:
                    pass
        hops_legend = sorted((h, f"{h}: hop{h}") for h in seen_hops)

    # Helper: extract a timeseries for a single DS name from rows
    def extract_series(ds_name: str):
        col = name_to_idx.get(ds_name, None)
        if col is None:
            # DS missing in this RRD (e.g., old schema) → produce aligned nulls
            return [None] * len(rows)
        out_vals = []
        for r in rows:
            try:
                val = r[col]
            except Exception:
                val = None
            out_vals.append(_nan_to_none(val))
        return out_vals

    # Build hop entries with per‑metric arrays
    hop_entries = []
    for hop_index, label_text in hops_legend:
        entry = {
            "hop": int(hop_index),
            "name": str(label_text),
            "color": _color(int(hop_index)),
            "metrics": {}
        }
        for m_schema, m_json in zip(schema_metrics, json_metrics):
            ds = f"hop{hop_index}_{m_schema}"
            entry["metrics"][m_json] = extract_series(ds)
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
        logger.info(f"[{ip}] exported {out_path} ({len(epochs)} points, hops={len(hop_entries)})")
    return out_path
