#!/usr/bin/env python3

# modules/graph_utils.py

import os
import re

def get_available_hops(ip, graph_dir="html/graphs"):
    """
    Returns a list of unique hop numbers found in graph PNG filenames for a given IP.

    Example matched filename: 8.8.8.8_hop0_avg_1h.png → hop0 → 0

    Args:
        ip (str): The target IP address
        graph_dir (str): Path to the graph output directory

    Returns:
        List[int]: Sorted list of hop numbers like [0, 1, 2, ...]
    """
    hops = set()
    if not os.path.exists(graph_dir):
        return []

    for fname in os.listdir(graph_dir):
        # Match: <ip>_hop<number>_<any_metric>_<any_range>.png
        match = re.match(rf"{re.escape(ip)}_hop(\d+)_\w+_", fname)
        if match:
            hops.add(int(match.group(1)))

    return sorted(hops)
