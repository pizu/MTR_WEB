#!/usr/bin/env python3
import os
import json
from modules.utils import resolve_all_paths

# ---- tuning knobs ----
UNSTABLE_THRESHOLD = 0.45   # if top host share < 45% → label as "varies(...)"
TOPK_TO_SHOW       = 3      # list up to 3 hosts inside "varies(...)"
MAJORITY_WINDOW    = 200    # soft cap on total samples kept per hop (decay oldest)
STICKY_MIN_WINS    = 3      # hysteresis: require N wins to flip sticky label
IGNORE_HOSTS       = set()  # keep ???; add "_gateway" here if you want to ignore it

def _paths(ip, settings):
    """
    Resolve per-target artifact paths honoring settings['paths']['traceroute'].
    """
    paths = resolve_all_paths(settings or {})
    d = paths.get("traceroute") or "traceroute"
    os.makedirs(d, exist_ok=True)
    stem = os.path.join(d, ip)
    return {
        "txt":   f"{stem}.trace.txt",
        "json":  f"{stem}.json",          # legacy hopN→host map
        "stats": f"{stem}_hops_stats.json",
        "hops":  f"{stem}_hops.json",     # list[{count, host}]
    }

def _load_stats(p):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_stats(p, stats):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

def _write_hops_json(stats, hops_path):
    """
    Convert rolling stats -> compact [{count, host}] labels file.
    """
    labels = []
    for hop_str, s in sorted(stats.items(), key=lambda kv: int(kv[0])):
        try:
            hop_int = int(hop_str)
        except Exception:
            continue
        if hop_int < 1:
            continue

        RESERVED_KEYS = {"_order", "last", "wins"}
        items = [(k, v) for k, v in s.items()
         if isinstance(v, int) and k not in RESERVED_KEYS and k not in IGNORE_HOSTS]
        total = sum(c for _, c in items)
        if total == 0:
            continue

        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total

        if share < UNSTABLE_THRESHOLD and len(items) >= 2:
            sample = ", ".join(h for h, _ in items[:TOPK_TO_SHOW])
            host_label = f"varies ({sample})"
        else:
            host_label = s.get("last") or top_host

        labels.append({"count": hop_int, "host": host_label})

    if labels:
        with open(hops_path, "w", encoding="utf-8") as f:
            json.dump(labels, f, indent=2)

def update_hop_labels_only(ip, hops, settings, logger):
    """
    Re-write <ip>_hops.json from EXISTING stats ONLY (no stat mutation here).
    Rolling stats are maintained by other components to avoid double counting.
    """
    p = _paths(ip, settings)
    stats = _load_stats(p["stats"])
    _write_hops_json(stats, p["hops"])
    if logger:
        logger.debug(f"[{ip}] Hop labels refreshed (no stat changes) -> {p['hops']}")

def save_trace_and_json(ip, hops, settings, logger):
    """
    Saves traceroute results for a target in two formats (plus 2 auxiliary files):

    1) <target>.trace.txt — text lines: "<hop> <ip/host> <avg> ms"
    2) <target>.json      — dict: {"hop1": "1.2.3.4", "hop2": "5.6.7.8", ...}

    Aux (maintained elsewhere):
    3) <target>_hops_stats.json — rolling counts per hop for label stabilization
    4) <target>_hops.json       — list[{count, host}]
    """
    p = _paths(ip, settings)

    # 1) Plain text — skip hop0
    with open(p["txt"], "w", encoding="utf-8") as f:
        for hop in hops:
            try:
                hop_num = int(hop.get("count", 0))
            except (TypeError, ValueError):
                continue
            if hop_num < 1:
                continue
            ip_addr = hop.get("host", "?")
            latency = hop.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")
    if logger:
        logger.info(f"Saved traceroute to {p['txt']}")

    # 2) Legacy hopN → host map — skip hop0
    hop_map = {}
    for hop in hops:
        try:
            hop_num = int(hop.get("count", 0))
        except (TypeError, ValueError):
            continue
        if hop_num < 1:
            continue
        hop_map[f"hop{hop_num}"] = hop.get("host", f"hop{hop_num}")
    with open(p["json"], "w", encoding="utf-8") as f:
        json.dump(hop_map, f, indent=2)
    if logger:
        logger.info(f"Saved hop label map to {p['json']}")

    # 3) Refresh stabilized labels from existing stats (no stat mutation here)
    update_hop_labels_only(ip, hops, settings, logger)
