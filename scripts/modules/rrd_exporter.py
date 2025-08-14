#!/usr/bin/env python3
"""
modules/rrd_exporter.py

Exports interactive-friendly JSON time series for a target IP and a given time range label.
Output path: html/data/<ip>_<label>.json

JSON schema:
{
  "ip": "1.1.1.1",
  "label": "1h",
  "seconds": 3600,
  "step": 60,
  "timestamps": ["10:00","10:01", ...],  # human-friendly HH:MM
  "epoch": [1723545600, 1723545660, ...],
  "hops": [
    {
      "hop": 0,
      "name": "0: 192.0.2.1",          # from traceroute labels (stabilized)
      "color": "#60a5fa",               # deterministic per hop
      "metrics": {
        "avg":  [.., .., ..],
        "last": [.., .., ..],
        "best": [.., .., ..],
        "loss": [.., .., ..]
      }
    },
    ...
  ]
}
"""

import os
import math
import time
import json
import rrdtool
from datetime import datetime
from modules.graph_utils import get_labels  # hop legend labels

def _color(hop_index: int) -> str:
    """Deterministic color per hop (matches graph_workers)."""
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"#{r:02x}{g:02x}{b:02x}"

def _fmt_ts(epoch: int) -> str:
    """Human-friendly HH:MM timestamps for the x-axis."""
    try:
        return datetime.fromtimestamp(epoch).strftime("%H:%M")
    except Exception:
        return ""

def _nan_to_none(v):
    """Convert NaN/None/non-numeric to None for JSON."""
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
    RRD_DIR   = settings.get("rrd_directory", "rrd")
    HTML_DIR = resolve_html_dir(settings)
    DATA_DIR = os.path.join(HTML_DIR, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
    _ensure_dir(os.path.join(DATA_DIR, "x"))  # ensure folder exists

    # Metrics to export (names only; e.g., ["avg", "last", "best", "loss"])
    metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]
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

    # Build legend labels: list of (hop_index:int, "hop: label")
    hops_legend = get_labels(ip, traceroute_dir=settings.get("traceroute_directory", "traceroute")) or []
    # If no labels (rare), still try to enumerate hop indices by inspecting DS names after fetch.

    # Fetch once for the time grid + all DS columns available in the chosen RRA
    end = int(time.time())
    start = end - int(seconds)

    try:
        # Correct signature: returns ((start, end, step), names, rows)
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
    # rows length may be (f_end - f_start) / step; construct epochs to match rows
    # Guard against weirdness if step is None/0
    if step and step > 0:
        epochs = list(range(int(f_start), int(f_end), step))
    else:
        # Fallback: derive length from rows only
        epochs = list(range(len(rows)))
    # If lengths mismatch, trim to common length
    if len(epochs) != len(rows):
        n = min(len(epochs), len(rows))
        epochs = epochs[:n]
        rows = rows[:n]

    labels_hhmm = [_fmt_ts(ts if step else int(time.time())) for ts in epochs]

    # Build a quick index for DS names -> column index
    name_to_idx = {name: i for i, name in enumerate(names or [])}

    # If we didn't get any legend labels, infer hop indices from DS names (e.g., hop0_avg)
    if not hops_legend and names:
        seen_hops = set()
        for nm in names:
            # expect "hopN_metric"
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
            return [None] * len(rows)
        out_vals = []
        for r in rows:
            # each row is a tuple aligned with names; r[col] can be None/NaN
            try:
                val = r[col]
            except Exception:
                val = None
            out_vals.append(_nan_to_none(val))
        return out_vals

    # Build hop entries with per-metric arrays
    hop_entries = []
    for hop_index, label_text in hops_legend:
        entry = {
            "hop": int(hop_index),
            "name": str(label_text),
            "color": _color(int(hop_index)),
            "metrics": {}
        }
        for m in metrics:
            ds = f"hop{hop_index}_{m}"
            entry["metrics"][m] = extract_series(ds)
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
