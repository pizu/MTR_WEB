#!/usr/bin/env python3

# modules/fping_status.py

import subprocess

def get_fping_status(ip, fping_path):
    """
    Pings the IP using fping to check if it's reachable.

    Returns:
        "Reachable", "Unreachable", or "Unknown"
    """
    if not fping_path:
        return "Unknown"

    try:
        result = subprocess.run(
            [fping_path, "-c1", "-t500", ip],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return "Reachable" if result.returncode == 0 else "Unreachable"
    except Exception:
        return "Unknown"
