#!/usr/bin/env python3
"""
modules/graph_utils.py

Hop label stabilization for legends (“varies(...)” when path is unstable) and
helpers for traceroute artifacts used by the Chart.js UI.

What’s in here:
- update_hop_labels_only(ip, hops, settings, logger): update stabilized labels JSON.
- save_trace_and_json(ip, hops, settings, logger): write human trace + legacy hop map,
  then refresh stabilized labels.
- get_labels(ip, traceroute_dir): read stabilized labels for legends.
- get_available_hops(ip, traceroute_dir): derive hop indices from stabilized labels.

What changed:
- Removed legacy fallback that scanned for per-hop PNG files. No dependency on
  “_hop*.png” remains.
- Added calendar-ready logging of hop path changes (events + intervals) as JSONL.
  This is disabled by default; enable via settings['calendar']['hop_change_events_enabled'].

Inputs:
- 'hops' is the normalized list from mtr_runner.run_mtr(...) with keys:
  count, host, Loss%, Snt, Last, Avg, Best, Wrst, StDev

Artifacts (under traceroute_directory, default "traceroute/"):
- <ip>.trace.txt          : human-readable traceroute
- <ip>.json               : legacy hopN → host JSON
- <ip>_hops_stats.json    : rolling stats to decide “varies(...)”
- <ip>_hops.json          : stabilized labels for legends  [{count:int, host:str}, ...]
- <ip>_path_state.json    : current active path and since-when (for interval logging)

Calendar logs (under log_directory, default "logs/"):
- hop_change_events/<ip>.jsonl     : event stream (old/new path snapshots with timestamp)
- hop_change_intervals/<ip>.jsonl  : closed intervals for which a path was active
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

# ---- tuning knobs for "varies(...)" ----
UNSTABLE_THRESHOLD = 0.45   # top host share <45% → "varies(...)"
TOPK_TO_SHOW       = 3      # show up to N hosts inside varies(...)
MAJORITY_WINDOW    = 200    # soft cap; oldest counts decay
STICKY_MIN_WINS    = 3      # hysteresis to avoid flip-flop
IGNORE_HOSTS       = set()  # keep "???"; add "_gateway" here if you want to hide it


# --------- Path helpers ---------
def _paths(ip: str, settings: dict) -> Dict[str, str]:
    d = settings.get("traceroute_directory", "traceroute")
    os.makedirs(d, exist_ok=True)
    stem = os.path.join(d, ip)
    return {
        "txt":        f"{stem}.trace.txt",
        "json":       f"{stem}.json",
        "stats":      f"{stem}_hops_stats.json",
        "hops":       f"{stem}_hops.json",
        "path_state": f"{stem}_path_state.json",
    }


def _calendar_dirs(settings: dict) -> Tuple[str, str]:
    log_dir = settings.get("log_directory", "logs")
    events_dir = os.path.join(log_dir, "hop_change_events")
    intervals_dir = os.path.join(log_dir, "hop_change_intervals")
    os.makedirs(events_dir, exist_ok=True)
    os.makedirs(intervals_dir, exist_ok=True)
    return events_dir, intervals_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------- JSON I/O helpers ---------
def _load_json(path: str, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def _append_jsonl(path: str, record: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")


# --------- Stabilization core ---------
def _load_stats(p_stats: str) -> dict:
    return _load_json(p_stats, {})


def _save_stats(p_stats: str, stats: dict) -> None:
    _save_json(p_stats, stats)


def _update_stats_with_snapshot(stats: dict, hops: List[dict]) -> dict:
    """
    Update per-hop “vote counters” with a new MTR snapshot, with decay and hysteresis.
    """
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
            s["_order"].insert(0, host)  # newest first
        s[host] += 1

        # decay oldest to keep within window
        total = sum(v for k, v in s.items() if isinstance(v, int))
        if total > MAJORITY_WINDOW:
            for key in list(s["_order"])[::-1]:
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

        # sticky majority
        modal = max((k for k in s if isinstance(s.get(k), int)), key=lambda k: s[k], default=None)
        cur = s.get("last")
        if cur is None:
            s["last"] = modal
            s["wins"] = 1
        elif modal == cur:
            s["wins"] = min(s.get("wins", 0) + 1, STICKY_MIN_WINS)
        else:
            s["wins"] = s.get("wins", 0) - 1
            if s["wins"] <= 0:
                s["last"] = modal
                s["wins"] = 1
    return stats


def _build_label_for_bucket(bucket: dict) -> Optional[str]:
    """
    Decide whether a hop gets a concrete host or “varies (a, b, c)”.
    """
    # collect (host, count) entries ignoring control keys
    items = [(k, bucket[k]) for k in bucket if isinstance(bucket.get(k), int) and k not in IGNORE_HOSTS]
    total = sum(c for _, c in items)
    if total == 0:
        return None
    items.sort(key=lambda kv: -kv[1])
    top_host, top_count = items[0]
    share = top_count / total

    if share < UNSTABLE_THRESHOLD and len(items) >= 2:
        sample = ", ".join(h for h, _ in items[:TOPK_TO_SHOW])
        return f"varies ({sample})"
    return bucket.get("last") or top_host


def _write_hops_json(stats: dict, p_hops: str) -> Tuple[List[dict], List[str]]:
    """
    Write stabilized labels JSON and also return:
      - labels_json: [{count:int, host:str}, ...]
      - path_list  : ["192.0.2.1","varies (...)", ...] ordered by hop
    """
    labels_json: List[dict] = []
    path_list: List[str] = []
    for hop_str, bucket in sorted(stats.items(), key=lambda kv: int(kv[0])):
        host_label = _build_label_for_bucket(bucket)
        if host_label is None:
            continue
        count = int(hop_str)
        labels_json.append({"count": count, "host": host_label})
        path_list.append(host_label)

    if labels_json:
        _save_json(p_hops, labels_json)
    return labels_json, path_list


# --------- Calendar / interval logging ---------
def _record_hop_change_if_needed(ip: str, path_list: List[str], settings: dict, logger) -> None:
    """
    If the stabilized path changed, write:
      - an event record to logs/hop_change_events/<ip>.jsonl
      - a closed interval to logs/hop_change_intervals/<ip>.jsonl
    And rotate traceroute/<ip>_path_state.json to the new active path.

    Controlled by settings['calendar']['hop_change_events_enabled'] (default False).
    """
    cal_cfg = (settings.get("calendar") or {})
    if not bool(cal_cfg.get("hop_change_events_enabled", False)):
        return  # feature is off

    p = _paths(ip, settings)
    state = _load_json(p["path_state"], {})
    old_path = state.get("path") or []
    active_since = state.get("active_since")  # ISO string

    if path_list == old_path:
        return  # no change

    # Paths & dirs
    events_dir, intervals_dir = _calendar_dirs(settings)
    events_file = os.path.join(events_dir, f"{ip}.jsonl")
    intervals_file = os.path.join(intervals_dir, f"{ip}.jsonl")
    now_iso = _utc_now_iso()

    # 1) Event (old → new)
    _append_jsonl(events_file, {
        "ts": now_iso,
        "ip": ip,
        "event": "hop_path_changed",
        "old": old_path,
        "new": path_list
    })

    # 2) Close previous interval if present
    if active_since:
        _append_jsonl(intervals_file, {
            "start": active_since,
            "end": now_iso,
            "ip": ip,
            "path": old_path
        })

    # 3) Start new active interval
    _save_json(p["path_state"], {
        "active_since": now_iso,
        "path": path_list
    })

    if logger:
        logger.info(f"[{ip}] Hop path changed → interval closed; new path active since {now_iso}")


# --------- Public functions ---------
def update_hop_labels_only(ip: str, hops: List[dict], settings: dict, logger) -> None:
    """
    Refresh <ip>_hops_stats.json and <ip>_hops.json based on one MTR snapshot.
    Also emits calendar-ready hop-change logs (events + intervals) if enabled.
    """
    p = _paths(ip, settings)
    stats = _load_stats(p["stats"])
    stats = _update_stats_with_snapshot(stats, hops)
    _save_stats(p["stats"], stats)

    labels_json, path_list = _write_hops_json(stats, p["hops"])

    # Optional calendar logging
    try:
        _record_hop_change_if_needed(ip, path_list, settings, logger)
    except Exception as e:
        if logger:
            logger.warning(f"[{ip}] hop-change logging failed: {e}")

    if logger:
        logger.debug(f"[{ip}] Hop labels updated -> {p['hops']}")


def save_trace_and_json(ip: str, hops: List[dict], settings: dict, logger) -> None:
    """
    Saves:
      1) <ip>.trace.txt — human-readable traceroute
      2) <ip>.json      — legacy hopN → host map (kept)
    Also refreshes <ip>_hops_stats.json + <ip>_hops.json and records hop-change events.
    """
    p = _paths(ip, settings)

    # 1) human-readable .trace.txt
    with open(p["txt"], "w", encoding="utf-8") as f:
        for hop in hops:
            hop_num = hop.get("count", "?")
            ip_addr = hop.get("host", "?")
            latency = hop.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")
    if logger:
        logger.info(f"Saved traceroute to {p['txt']}")

    # 2) legacy hop map
    hop_map = {f"hop{hop['count']}": hop.get("host", f"hop{hop['count']}") for hop in hops if "count" in hop}
    _save_json(p["json"], hop_map)
    if logger:
        logger.info(f"Saved hop label map to {p['json']}")

    # 3) stabilization + optional calendar logging
    update_hop_labels_only(ip, hops, settings, logger)


def get_available_hops(ip: str, graph_dir: str = "html/graphs", traceroute_dir: str = "traceroute") -> List[int]:
    """
    Returns a sorted list of hop indices for a target, based ONLY on the new
    stabilized labels store (traceroute/<ip>_hops.json). No per-hop PNG fallback.
    """
    json_path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, encoding="utf-8") as f:
                arr = json.load(f)
            return sorted({int(item["count"]) for item in arr if isinstance(item, dict) and "count" in item})
        except Exception:
            return []
    return []


def get_labels(ip: str, traceroute_dir: str = "traceroute") -> List[Tuple[int, str]]:
    """
    Return a list of (hop_number:int, label:str) pairs for legends.

    Preferred source: traceroute/<ip>_hops.json
      [
        {"count": 1, "host": "192.0.2.1"},
        {"count": 2, "host": "varies (203.0.113.1, 203.0.113.9, ???)"},
        ...
      ]

    Fallback (legacy): traceroute/<ip>.trace.txt with lines like:
      "<hop> <ip/name> <avg_ms> ms"
    """
    # 1) Stabilized JSON with “varies(…)”
    json_path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, encoding="utf-8") as f:
                arr = json.load(f)
            out: List[Tuple[int, str]] = []
            for item in arr:
                if not isinstance(item, dict):
                    continue
                if "count" not in item or "host" not in item:
                    continue
                hop = int(item["count"])
                host = str(item["host"])
                out.append((hop, f"{hop}: {host}"))
            return sorted(out, key=lambda x: x[0])
        except Exception:
            pass  # fall through to legacy

    # 2) Legacy plain-text fallback
    txt_path = os.path.join(traceroute_dir, f"{ip}.trace.txt")
    if os.path.exists(txt_path):
        out: List[Tuple[int, str]] = []
        try:
            with open(txt_path, encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        try:
                            hop = int(parts[0])
                            host = parts[1]
                            out.append((hop, f"{hop}: {host}"))
                        except ValueError:
                            continue
            return sorted(out, key=lambda x: x[0])
        except Exception:
            return []
    return []
