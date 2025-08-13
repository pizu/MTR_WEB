#!/usr/bin/env python3

# modules/graph_utils.py

import os
import re
import json

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
        match = re.match(rf"{re.escape(ip)}_hop(\d+)_\w+_", fname)
        if match:
            hops.add(int(match.group(1)))
            
    return sorted(hops)

def get_labels(ip, traceroute_dir="traceroute"):
    """
    Loads hop labels from a traceroute file (e.g. 1.1.1.1.trace.txt)
    Returns a list of tuples: (hop_number, label)

    Example return: [(0, 'Hop 0 - 192.0.2.1'), (1, 'Hop 1 - 203.0.113.1')]
    """
    json_path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    if os.path.exists(json_path):
        try:
            arr = json.loads(open(json_path, encoding="utf-8").read())
            out = []
            for item in arr:
                if not isinstance(item, dict): 
                    continue
                hop = item.get("count")
                host = item.get("host")
                if hop is None or host is None:
                    continue
                # show "N: host" exactly as requested
                out.append((int(hop), f"{int(hop)}: {host}"))
            return sorted(out, key=lambda x: x[0])
            
        except Exception:
            pass  # fall through to legacy

    # Legacy fallback: parse "<ip>.trace.txt"
    path = os.path.join(traceroute_dir, f"{ip}.trace.txt")
    if not os.path.exists(path):
        return []
    hops = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=2)
            if len(parts) >= 2:
                try:
                    hop_num = int(parts[0])
                    hop_ip = parts[1]
                    hops.append((hop_num, f"{hop_num}: {hop_ip}"))
                    
                except ValueError:
                    continue
    return hops
