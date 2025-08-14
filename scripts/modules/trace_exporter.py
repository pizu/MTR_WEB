#!/usr/bin/env python3
import os
import json

# ---- tuning knobs ----
UNSTABLE_THRESHOLD = 0.45   # if top host share < 45% → label as "varies(...)"
TOPK_TO_SHOW       = 3      # list up to 3 hosts inside "varies(...)"
MAJORITY_WINDOW    = 200    # soft cap on total samples kept per hop (decay oldest)
STICKY_MIN_WINS    = 3      # hysteresis: require N wins to flip sticky label
IGNORE_HOSTS       = set()  # keep ???; add "_gateway" here if you want to ignore it

def _paths(ip, settings):
    d = settings.get("traceroute_directory", "traceroute")
    os.makedirs(d, exist_ok=True)
    stem = os.path.join(d, ip)
    return {
        "txt":   f"{stem}.trace.txt",
        "json":  f"{stem}.json",          # legacy hopN→host map
        "stats": f"{stem}_hops_stats.json",
        "hops":  f"{stem}_hops.json",     # list[{count, host}] with varies(...)
    }

def _load_stats(p):
    try:    return json.loads(open(p, encoding="utf-8").read())
    except: return {}

def _save_stats(p, stats):
    open(p, "w", encoding="utf-8").write(json.dumps(stats, indent=2))

def _write_hops_json(stats, hops_path):
    """
    Convert rolling stats -> compact [{count, host}] labels file.
    Ignores any hop < 1 (filters out legacy hop0 if it exists on disk).
    """
    labels = []
    for hop_str, s in sorted(stats.items(), key=lambda kv: int(kv[0])):
        hop_int = int(hop_str)
        if hop_int < 1:
            continue
        items = [(k, s[k]) for k in s if isinstance(s.get(k), int) and k not in IGNORE_HOSTS]
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
        open(hops_path, "w", encoding="utf-8").write(json.dumps(labels, indent=2))

def update_hop_labels_only(ip, hops, settings, logger=None):
    """
    Re-write <ip>_hops.json from EXISTING stats ONLY.
    (No updates here to avoid double counting — stats are maintained in monitor.py)
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
    4) <target>_hops.json       — list[{count, host}] with stable or "varies(...)" labels
    """
    p = _paths(ip, settings)

    # 1) Plain text (human-readable), skipping hop0
    with open(p["txt"], "w") as f:
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
    logger.info(f"Saved traceroute to {p['txt']}")

    # 2) Simple hopN → host map (legacy/kept), skipping hop0
    hop_map = {}
    for hop in hops:
        try:
            hop_num = int(hop.get("count", 0))
        except (TypeError, ValueError):
            continue
        if hop_num < 1:
            continue
        hop_map[f"hop{hop_num}"] = hop.get("host", f"hop{hop_num}")
    with open(p["json"], "w") as f:
        json.dump(hop_map, f, indent=2)
    logger.info(f"Saved hop label map to {p['json']}")
