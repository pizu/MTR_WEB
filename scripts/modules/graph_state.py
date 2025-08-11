#!/usr/bin/env python3

"""
Tiny JSON state for run index (used for cadence throttling).
"""
import json
import os

def load_run_index(state_path: str) -> int:
    try:
        with open(state_path, "r") as f:
            data = json.load(f)
            return int(data.get("run_index", 0))
    except Exception:
        return 0

def save_run_index(state_path: str, idx: int) -> None:
    try:
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w") as f:
            json.dump({"run_index": int(idx)}, f)
    except Exception:
        pass
