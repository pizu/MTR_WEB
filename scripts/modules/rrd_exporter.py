#!/usr/bin/env python3
"""
modules/rrd_exporter.py

Exports interactive-friendly JSON time series for a target IP and a given time range label.
Output lives under: html/data/<ip>_<label>.json

Why JSON?
- Your site is static. We can fetch JSON from the browser and render with Chart.js.
- A single JSON per (ip, timerange) contains *all* metrics. The UI flips metric client-side.

JSON shape:
{
  "ip": "1.1.1.1",
  "label": "1h",
  "seconds": 3600,
  "step": 60,
  "timestamps": [ "10:00", "10:01", ... ],        # strings for x-axis
  "epoch":     [ 1723545600, 1723545660, ... ],  # raw epoch (optional; useful later)
  "hops": [
    { "hop": 0, "name": "0: 192.0.2.1", "color": "#60a5fa",
      "metrics": { "avg": [..], "last": [..], "best": [..], "loss": [..] } },
    ...
  ]
}

Notes:
- Loss is in %; latency metrics in ms (as per your DS naming).
- Missing data -> null (keeps gaps visible).
- Colors are deterministic per hop index (same formula as graph_workers.py).
"""

import os, re, math, time, json, rrdtool
from datetime import datetime, timezone
from modules.graph_utils import get_labels  # to map hop index -> legend label

def _color(hop_index: int) -> str:
    r = int((1 + math.sin(hop_index * 0.3)) * 127)
    g = int((1 + math.sin(hop_index * 0.3 + 2)) * 127)
    b = int((1 + math.sin(hop_index * 0.3 + 4)) * 127)
    return f"#{r:02x}{g:02x}{b:02x}"

def _fmt_ts(epoch: int) -> str:
    # compact local time label like "10:05"
    return datetime.fromtimestamp(epoch).strftime("%H:%M")

def _nan_to_none(v):
    try:
        if v is None: return None
        if isinstance(v, float) and (v != v):  # NaN check
            return None
        return float(v)
    except Exception:
        return None

def export_ip_timerange_json(ip: str, settings: dict, label: str, seconds: int, logger=None) -> str:
    """
    Export a single JSON for (ip, label, seconds). Returns the output path.
    """
    RRD_DIR   = settings.get("rrd_directory", "rrd")
    GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
    DATA_DIR  = os.path.join("html", "data")
    os.makedirs(DATA_DIR, exist_ok=True)

    # Which metrics we export (names only; e.g., ["avg","last","best","loss"])
    metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]

    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    if not os.path.exists(rrd_path):
        if logger: logger.warning(f"[{ip}] RRD missing: {rrd_path}")
        # write a stub so UI shows "no data"
        out = {
            "ip": ip, "label": label, "seconds": seconds, "step": None,
            "timestamps": [], "epoch": [], "hops": []
        }
        out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")
        open(out_path, "w", encoding="utf-8").write(json.dumps(out, indent=2))
        return out_path

    # Legend: list[(hop_index, "idx: hostlabel")]
    hops_legend = get_labels(ip, traceroute_dir=settings.get("traceroute_directory", "traceroute"))
    if not hops_legend:
        hops_legend = []  # UI will show empty

    # Figure out DS names available
    info = rrdtool.info(rrd_path)
    ds_available = set()
    for k in info.keys():
        if k.startswith("ds[") and k.endswith("].type"):
            ds_available.add(k[3:-6])  # 'ds[hop0_avg].type' → 'hop0_avg'

    # Determine fetch window: end now, start -seconds
    end = int(time.time())
    start = end - int(seconds)

    # We’ll build a list of timestamps once, from any DS we can fetch.
    # Prefer hop0 and first metric for step; if missing, fallback to any ds.
    step = None
    base_ds = None
    for m in metrics:
        cand = f"hop0_{m}"
        if cand in ds_available:
            base_ds = cand
            break
    if base_ds is None and ds_available:
        base_ds = sorted(ds_available)[0]

    # If absolutely no DS, write empty
    if base_ds is None:
        out = {
            "ip": ip, "label": label, "seconds": seconds, "step": None,
            "timestamps": [], "epoch": [], "hops": []
        }
        out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")
        open(out_path, "w", encoding="utf-8").write(json.dumps(out, indent=2))
        return out_path

    # Fetch base to get the time grid
    try:
        (f_start, f_end, f_step), f_rows = rrdtool.fetch(rrd_path, "AVERAGE",
                                                         "--start", str(start), "--end", str(end),
                                                         f"DEF:v={rrd_path}:{base_ds}:AVERAGE")
    except rrdtool.OperationalError:
        # Some rrdtool builds require different fetch signature; fallback w/out DEF
        (f_start, f_end, f_step), f_rows = rrdtool.fetch(rrd_path, "AVERAGE",
                                                         "--start", str(start), "--end", str(end))
    step = int(f_step)
    epochs = list(range(int(f_start), int(f_end), step))
    labels = [_fmt_ts(ts) for ts in epochs]

    # Helper to fetch a specific DS onto the base grid
    def fetch_ds(ds_name: str):
        if ds_name not in ds_available:
            return [None] * len(epochs)
        try:
            (s, e, st), rows = rrdtool.fetch(rrd_path, "AVERAGE",
                                             "--start", str(start), "--end", str(end),
                                             f"DEF:v={rrd_path}:{ds_name}:AVERAGE")
        except rrdtool.OperationalError:
            # Fallback: try raw fetch and assume rows align; if not, pad
            (s, e, st), rows = rrdtool.fetch(rrd_path, "AVERAGE",
                                             "--start", str(start), "--end", str(end))
        # Flatten values; rows is list of tuples per timestamp
        vals = [r[0] if r and len(r) else None for r in rows]
        # If alignment differs, pad/trim to match epochs length
        if len(vals) != len(epochs):
            # naive align by trunc/pad; acceptable for static UI
            if len(vals) > len(epochs): vals = vals[-len(epochs):]
            else: vals = ([None] * (len(epochs) - len(vals))) + vals
        return [_nan_to_none(v) for v in vals]

    # Build hop datasets {metrics: {metricName: [..]}}
    hop_entries = []
    for hop_index, label_text in hops_legend:
        entry = {
            "hop": int(hop_index),
            "name": str(label_text),
            "color": _color(int(hop_index)),
            "metrics": {}
        }
        for m in metrics:
            ds_name = f"hop{hop_index}_{m}"
            entry["metrics"][m] = fetch_ds(ds_name)
        hop_entries.append(entry)

    out = {
        "ip": ip,
        "label": label,
        "seconds": int(seconds),
        "step": step,
        "timestamps": labels,
        "epoch": epochs,
        "hops": hop_entries
    }

    out_path = os.path.join(DATA_DIR, f"{ip}_{label}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    if logger:
        logger.info(f"[{ip}] exported {out_path} ({len(epochs)} points)")
    return out_path
