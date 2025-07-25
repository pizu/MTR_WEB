#!/usr/bin/env python3
import os
import re

def get_available_hops(ip, graph_dir="html/graphs"):
    hops = set()
    for fname in os.listdir(graph_dir):
        match = re.match(rf"{re.escape(ip)}_hop(\d+)_avg_", fname)
        if match:
            hops.add(int(match.group(1)))
    return sorted(hops)
