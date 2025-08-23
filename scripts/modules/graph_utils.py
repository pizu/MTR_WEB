#!/usr/bin/env python3
"""
modules/graph_utils.py (STRICT PATHS)

- Stabilizes hop labels: writes <ip>_hops.json as "N: host" where host can be "varies (a, b)".
- Tracks rolling per-hop votes in <ip>_hops_stats.json (NO DB writes unless called).
- Emits calendar-friendly logs (events/intervals) under settings['paths']['logs'].

STRICT POLICY:
- Traceroute dir strictly from settings['paths']['traceroute']; no defaults.
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
from modules.utils import resolve_all_paths

UNSTABLE_THRESHOLD = 0.45
TOPK_TO_SHOW       = 3
MAJORITY_WINDOW    = 200
STICKY_MIN_WINS    = 3
IGNORE_HOSTS       = set()
RESERVED_KEYS      = {"_order", "last", "wins"}

def _load_json(p: str, default):
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(p: str, data) -> None:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def _strict_tr_dir(settings: dict) -> str:
    paths = resolve_all_paths(settings or {})
    d = (settings or {}).get("paths", {}).get("traceroute")
    if not d or not os.path.isdir(d):
        raise FileNotFoundError("settings['paths']['traceroute'] is missing or does not exist.")
    return d

def _paths(ip: str, settings: dict) -> Dict[str, str]:
    d = _strict_tr_dir(settings)
    stem = os.path.join(d, ip)
    return {
        "txt":        f"{stem}.trace.txt",
        "json":       f"{stem}.json",
        "stats":      f"{stem}_hops_stats.json",
        "hops":       f"{stem}_hops.json",
        "path_state": f"{stem}_path_state.json",
    }

def _calendar_dirs(settings: dict):
    paths = resolve_all_paths(settings or {})
    log_dir = (settings or {}).get("paths", {}).get("logs") or paths.get("logs") or "logs"
    events_dir = os.path.join(log_dir, "hop_change_events")
    intervals_dir = os.path.join(log_dir, "hop_change_intervals")
    os.makedirs(events_dir, exist_ok=True)
    os.makedirs(intervals_dir, exist_ok=True)
    return events_dir, intervals_dir

def _load_stats(p_stats: str) -> dict:
    return _load_json(p_stats, {})

def _save_stats(p_stats: str, stats: dict) -> None:
    _save_json(p_stats, stats)

def _update_stats_with_snapshot(stats: dict, hops: List[dict]) -> dict:
    for h in hops:
        if "count" not in h:
            continue
        hop_str = str(int(h["count"]))
        host = h.get("host")
        if host is None:
            continue

        s = stats.setdefault(hop_str, {"_order": [], "last": None, "wins": 0})
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)
        s[host] += 1

        total = sum(v for k, v in s.items() if isinstance(v, int))
        if total > MAJORITY_WINDOW:
            for key in list(s["_order"])[::-1]:
                if isinstance(s.get(key), int) and total > MAJORITY_WINDOW:
                    s[key] = max(0, s[key] - 1)
                    total -= 1
                if total <= MAJORITY_WINDOW:
                    break

        items = [(k, v) for k, v in s.items()
                 if isinstance(v, int) and k not in RESERVED_KEYS and k not in IGNORE_HOSTS]
        items.sort(key=lambda kv: -kv[1])
        if not items:
            continue
        top_host, top_count = items[0]
        share = top_count / max(1, sum(v for _, v in items))

        last = s.get("last")
        if last == top_host:
            s["wins"] = min(STICKY_MIN_WINS, s.get("wins", 0) + 1)
        else:
            if s.get("wins", 0) >= STICKY_MIN_WINS or share >= 0.5:
                s["last"] = top_host
                s["wins"] = 1
            else:
                s["wins"] = max(0, s.get("wins", 0) - 1)
    return stats

def _write_hops_json(stats: dict, p_hops: str):
    labels = []
    path_endpoints = []
    for hop_str in sorted(stats.keys(), key=lambda x: int(x)):
        s = stats[hop_str]
        items = [(k, v) for k, v in s.items()
                 if isinstance(v, int) and k not in RESERVED_KEYS and k not in IGNORE_HOSTS]
        items.sort(key=lambda kv: -kv[1])
        if not items:
            continue

        top_host, top_count = items[0]
        total = sum(v for _, v in items)
        share = top_count / max(1, total)

        if share < UNSTABLE_THRESHOLD and len(items) >= 2:
            sample = ", ".join(k for k, _ in items[:TOPK_TO_SHOW])
            label_host = f"varies ({sample})"
        else:
            label_host = s.get("last") or top_host

        hop_num = int(hop_str)
        labels.append({"count": hop_num, "host": label_host})
        path_endpoints.append(label_host)

    _save_json(p_hops, labels)
    return labels, path_endpoints

def _emit_change_event(ip: str, hop: int, old: str, new: str, when_epoch: int, events_dir: str) -> None:
    ev = {"ip": ip, "hop": hop, "old": old, "new": new, "ts": when_epoch,
          "ts_iso": datetime.fromtimestamp(when_epoch, tz=timezone.utc).isoformat()}
    with open(os.path.join(events_dir, f"{ip}.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(ev) + "\n")

def _load_path_state(path_state_file: str) -> dict:
    return _load_json(path_state_file, {})

def _save_path_state(path_state_file: str, state: dict) -> None:
    _save_json(path_state_file, state)

def _maybe_emit_interval(ip: str, path_state_file: str, new_path: List[str],
                         now_epoch: int, intervals_dir: str, logger) -> None:
    state = _load_path_state(path_state_file)
    prev_sig = tuple(state.get("active_path", {}).get("labels", []))
    new_sig = tuple(new_path)
    if prev_sig == new_sig:
        return
    now_iso = datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat()
    if prev_sig:
        opened = int(state.get("active_path", {}).get("opened_ts", now_epoch))
        interval = {"ip": ip, "from_ts": opened, "to_ts": now_epoch,
                    "from_iso": datetime.fromtimestamp(opened, tz=timezone.utc).isoformat(),
                    "to_iso": now_iso, "labels": list(prev_sig)}
        with open(os.path.join(intervals_dir, f"{ip}.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(interval) + "\n")
    state["active_path"] = {"labels": list(new_sig), "opened_ts": now_epoch}
    _save_path_state(path_state_file, state)
    if logger:
        logger.info(f"[{ip}] Hop path changed → interval closed; new path active since {now_iso}")

def update_hop_labels_only(ip: str, hops: List[dict], settings: dict, logger) -> None:
    p = _paths(ip, settings)
    stats = _load_stats(p["stats"])
    stats = _update_stats_with_snapshot(stats, hops)
    _save_stats(p["stats"], stats)

    labels_json, path_list = _write_hops_json(stats, p["hops"])

    prev_labels = _load_json(p["hops"], [])
    prev_map = {int(x["count"]): x["host"] for x in prev_labels if isinstance(x, dict) and "count" in x and "host" in x}
    curr_map = {int(x["count"]): x["host"] for x in labels_json if isinstance(x, dict) and "count" in x and "host" in x}

    events_dir, intervals_dir = _calendar_dirs(settings)
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    for hop_num, new_label in curr_map.items():
        old_label = prev_map.get(hop_num)
        if old_label and old_label != new_label:
            _emit_change_event(ip, hop_num, old_label, new_label, now_epoch, events_dir)
    _maybe_emit_interval(ip, p["path_state"], path_list, now_epoch, intervals_dir, logger)

def save_trace_and_json(ip: str, hops: List[dict], settings: dict, logger) -> None:
    """Writer (kept, but STRICT path): human trace + legacy map + stabilized labels."""
    p = _paths(ip, settings)

    with open(p["txt"], "w", encoding="utf-8") as f:
        for h in hops:
            try:
                hop_num = int(h.get("count", 0))
            except (TypeError, ValueError):
                continue
            if hop_num < 1:
                continue
            ip_addr = h.get("host", "?")
            latency = h.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")

    hop_map = {}
    for h in hops:
        try:
            hop_num = int(h.get("count", 0))
        except (TypeError, ValueError):
            continue
        if hop_num < 1:
            continue
        hop_map[f"hop{hop_num}"] = h.get("host", f"hop{hop_num}")
    _save_json(p["json"], hop_map)

    update_hop_labels_only(ip, hops, settings, logger)

def get_labels(ip: str, traceroute_dir: Optional[str] = None,
               settings: Optional[dict] = None, logger=None) -> List[Tuple[int, str]]:
    # prefer explicit dir if valid; else STRICT from settings
    if traceroute_dir and os.path.isdir(traceroute_dir):
        d = traceroute_dir
    else:
        d = _strict_tr_dir(settings or {})
    stem = os.path.join(d, ip)

    p_hops = f"{stem}_hops.json"
    if os.path.exists(p_hops):
        try:
            data = _load_json(p_hops, [])
            out = []
            for item in data:
                if not isinstance(item, dict) or "count" not in item or "host" not in item:
                    continue
                hop = int(item["count"])
                host = str(item["host"])
                out.append((hop, f"{hop}: {host}"))
            out.sort(key=lambda t: t[0])
            return out
        except Exception:
            pass

    p_txt = f"{stem}.trace.txt"
    if os.path.exists(p_txt):
        out = []
        with open(p_txt, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2 and parts[0].isdigit():
                    hop = int(parts[0]); host = parts[1]
                    out.append((hop, f"{hop}: {host}"))
        out.sort(key=lambda t: t[0])
        return out
    return []

def get_available_hops(ip: str, traceroute_dir: Optional[str] = None,
                       settings: Optional[dict] = None,
                       graph_dir: Optional[str] = None, **kwargs) -> List[int]:
    if traceroute_dir and os.path.isdir(traceroute_dir):
        d = traceroute_dir
    else:
        d = _strict_tr_dir(settings or {})
    p_hops = os.path.join(d, f"{ip}_hops.json")
    data = _load_json(p_hops, [])
    hops = {int(item["count"]) for item in data if isinstance(item, dict) and "count" in item}
    return sorted(hops)
