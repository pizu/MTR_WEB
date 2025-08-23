#!/usr/bin/env python3
import os
import json

def _strict_tr_dir(settings):
    d = (settings or {}).get("paths", {}).get("traceroute")
    if not d or not os.path.isdir(d):
        raise FileNotFoundError("settings['paths']['traceroute'] is missing or does not exist.")
    return d

def _paths(ip, settings):
    d = _strict_tr_dir(settings)
    stem = os.path.join(d, ip)
    return {
        "txt":   f"{stem}.trace.txt",
        "json":  f"{stem}.json",
        "stats": f"{stem}_hops_stats.json",
        "hops":  f"{stem}_hops.json",
    }

def _load_stats(p):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_stats(p, stats):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

def _write_hops_json(stats, hops_path):
    UNSTABLE_THRESHOLD = 0.45
    TOPK_TO_SHOW = 3
    IGNORE_HOSTS = set()
    RESERVED_KEYS = {"_order", "last", "wins"}

    labels = []
    for hop_str, s in sorted(stats.items(), key=lambda kv: int(kv[0])):
        try:
            hop_int = int(hop_str)
        except Exception:
            continue
        if hop_int < 1:
            continue

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
        os.makedirs(os.path.dirname(hops_path), exist_ok=True)
        with open(hops_path, "w", encoding="utf-8") as f:
            json.dump(labels, f, indent=2)

def update_hop_labels_only(ip, hops, settings, logger):
    p = _paths(ip, settings)
    stats = _load_stats(p["stats"])
    _write_hops_json(stats, p["hops"])
    if logger:
        logger.debug(f"[{ip}] Hop labels refreshed (no stat changes) -> {p['hops']}")

def save_trace_and_json(ip, hops, settings, logger):
    p = _paths(ip, settings)

    # text
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

    # legacy map
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

    # rebuild stabilized labels from existing stats only
    update_hop_labels_only(ip, hops, settings, logger)
