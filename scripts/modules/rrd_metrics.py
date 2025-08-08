#!/usr/bin/env python3

# modules/rrd_metrics.py

import os
import rrdtool
from datetime import datetime

def get_rrd_metrics(ip, rrd_dir, data_sources):
    """
    Reads RRD data for the given IP and extracts metrics for:
    - hop0 only (first hop)
    - total average per metric across all hops

    Returns:
        (hop0_metrics: dict, avg_metrics: dict)
    """
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    if not os.path.exists(rrd_path):
        return {}, {}

    try:
        # Time range: last 2 minutes
        end = int(datetime.now().timestamp())
        start = end - 120
        (start_ts, end_ts, step), ds_names, rows = rrdtool.fetch(
            rrd_path, "AVERAGE", "--start", str(start), "--end", str(end)
        )

        # Get the most recent row with data
        latest = next((row for row in reversed(rows) if any(v is not None for v in row)), None)
        if not latest:
            return {}, {}

        hop0_metrics = {}
        total_metrics = {name: [] for name in data_sources}

        for i, ds_name in enumerate(ds_names):
            value = latest[i]
            if value is None:
                continue

            parts = ds_name.split("_")
            if len(parts) != 2:
                continue  # Ignore unexpected DS names

            hop_id, metric = parts
            if hop_id == "hop0":
                hop0_metrics[metric] = round(value, 1)
            total_metrics[metric].append(value)

        # Average all values for each metric (only numeric)
        avg_metrics = {}
        for metric, values in total_metrics.items():
            if not values:
                continue
            if metric == "loss":
                avg_metrics[metric] = round(sum(values), 1)  # loss is summed
            else:
                avg_metrics[metric] = round(sum(values) / len(values), 1)

        return hop0_metrics, avg_metrics

    except Exception as e:
        print(f"[RRD] Error extracting metrics for {ip}: {e}")
        return {}, {}
