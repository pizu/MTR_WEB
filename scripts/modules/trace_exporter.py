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
        "txt":  f"{stem}.trace.txt",
        "json": f"{stem}.json",          # your original hopN→host map (kept)
        "stats":f"{stem}_hops_stats.json",
        "hops": f"{stem}_hops.json",     # NEW: list[{count, host}] with varies(...)
    }

def _load_stats(p):
    try:    return json.loads(open(p, encoding="utf-8").read())
    except: return {}

def _save_stats(p, stats):
    open(p, "w", encoding="utf-8").write(json.dumps(stats, indent=2))

def _update_stats_with_snapshot(stats, hops):
    # hops items have at least: {"count": <int>, "host": <str (IP or '???')>, ...}
    for h in hops:
        if "count" not in h: continue
        hop = str(int(h["count"]))
        host = h.get("host")
        if host is None:
            continue  # keep '???' (it's a string), only skip real None
        s = stats.setdefault(hop, {"_order": [], "last": None, "wins": 0})
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)   # newest first
        s[host] += 1
        # decay to keep within MAJORITY_WINDOW
        total = sum(v for k,v in s.items() if isinstance(v, int))
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

def _write_hops_json(stats, hops_path):
    labels = []
    for hop_str, s in sorted(stats.items(), key=lambda kv: int(kv[0])):
        # include ??? in counts; only drop things in IGNORE_HOSTS
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
        labels.append({"count": int(hop_str), "host": host_label})
    if labels:
        open(hops_path, "w", encoding="utf-8").write(json.dumps(labels, indent=2))

def save_trace_and_json(ip, hops, settings, logger):
    """
    Saves traceroute results for a target in two formats (plus 2 new ones):

    1) <target>.trace.txt — text lines: "<hop> <ip/host> <avg> ms"
    2) <target>.json      — dict: {"hop0": "1.2.3.4", "hop1": "5.6.7.8", ...}    (kept)

    NEW:
    3) <target>_hops_stats.json — rolling counts per hop for label stabilization
    4) <target>_hops.json       — list[{count, host}] with stable or "varies(...)" labels
    """
    p = _paths(ip, settings)

    # 1) Plain text (human-readable)
    with open(p["txt"], "w") as f:
        for hop in hops:
            hop_num = hop.get("count", "?")
            ip_addr = hop.get("host", "?")
            latency = hop.get("Avg", "U")
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")
    logger.info(f"Saved traceroute to {p['txt']}")

    # 2) Simple hopN → host map (legacy/kept)
    hop_map = {f"hop{hop['count']}": hop.get("host", f"hop{hop['count']}") for hop in hops if "count" in hop}
    with open(p["json"], "w") as f:
        json.dump(hop_map, f, indent=2)
    logger.info(f"Saved hop label map to {p['json']}")

    # 3–4) Stabilized labels for charts/UIs
    stats = _load_stats(p["stats"])
    stats = _update_stats_with_snapshot(stats, hops)
    _save_stats(p["stats"], stats)
    _write_hops_json(stats, p["hops"])
    logger.info(f"Updated hop stats and labels: {p['hops']}")
