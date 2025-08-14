#!/usr/bin/env python3
"""
modules/monitor.py

Monitors one target in a loop:
  1) run MTR snapshot
  2) detect hop-path changes + loss changes
  3) update RRDs every iteration
  4) maintain rolling hop label stats (drives "varies (...)" labels)
  5) save traceroute JSON when something changed
"""

import os
import json
import time
from deepdiff import DeepDiff

from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, init_per_hop_rrds, update_rrd
from modules.severity import evaluate_severity_rules, hops_changed
from modules.trace_exporter import save_trace_and_json, update_hop_labels_only

# --- Label tunables ---
UNSTABLE_THRESHOLD = 0.45   # top share < threshold and competition -> "varies (...)"
TOPK_TO_SHOW       = 3
MAJORITY_WINDOW    = 200
STICKY_MIN_WINS    = 3
IGNORE_HOSTS       = set()

# ---------------- Small helpers for label files ----------------
def _label_paths(ip, settings):
    trace_dir = settings.get("traceroute_directory", "traceroute")
    os.makedirs(trace_dir, exist_ok=True)
    stem = os.path.join(trace_dir, ip)
    return stem + "_hops_stats.json", stem + "_hops.json"

def _load_stats(stats_path):
    try:
        with open(stats_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_stats(stats_path, stats):
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

# ---------------- Stats maintenance ----------------
def _update_stats_with_snapshot(stats, hops):
    """
    Update rolling counts keyed by hop index (string). Uses sticky modal logic
    and a soft decay window to avoid unbounded growth.
    """
    for h in hops:
        hop_idx = str(int(h.get("count", 0)))
        host = h.get("host")
        if host is None:
            continue

        s = stats.setdefault(hop_idx, {"_order": [], "last": None, "wins": 0})
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)
        s[host] += 1

        total_counts = sum(v for k, v in s.items() if isinstance(s.get(k), int))
        if total_counts > MAJORITY_WINDOW:
            for key in list(s["_order"])[::-1]:  # oldest first
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

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

# ---------------- Path-change stat hygiene ----------------
def _first_diff_index(prev_hops, curr_hops):
    """
    Return the first hop index (1-based) where host differs, or None if same up to min length.
    """
    n = min(len(prev_hops), len(curr_hops))
    for i in range(n):
        if (prev_hops[i] or {}).get("host") != (curr_hops[i] or {}).get("host"):
            return i + 1  # hops are 1-based in our data
    if len(prev_hops) != len(curr_hops):
        return n + 1
    return None

def _reset_stats_from(stats, start_hop_int, logger=None):
    """
    Drop stats for hop indices >= start_hop_int (1-based).
    """
    to_del = [k for k in stats.keys() if k.isdigit() and int(k) >= start_hop_int]
    for k in to_del:
        stats.pop(k, None)
    if logger:
        logger.debug(f"[labels] reset stats from hop {start_hop_int} (inclusive); removed {len(to_del)} entries")

def _realign_then_reset(stats, prev_hops, curr_hops, logger=None):
    """
    Best-effort migration:
      - Build map: last_modal_host -> old_hop_index
      - For each current hop, if its host matches a 'last' modal in the map,
        move that stats bucket to the new hop index.
      - Reset any leftover indexes that didn’t match.

    This reduces cross-hop mixing when one hop is inserted/removed, but isn’t perfect.
    """
    # Build reverse map from existing stats
    modal_to_oldidx = {}
    for idx_str, s in list(stats.items()):
        last = s.get("last")
        if isinstance(last, str):
            modal_to_oldidx.setdefault(last, []).append(int(idx_str))

    moved = 0
    new_stats = {}
    used_old = set()

    # Try to place existing buckets onto current indices by modal host
    for h in curr_hops:
        new_idx = int(h.get("count", 0))
        host = h.get("host")
        candidates = modal_to_oldidx.get(host) or []
        chosen = None
        for old_idx in candidates:
            if old_idx not in used_old:
                chosen = old_idx
                break
        if chosen is not None and str(chosen) in stats:
            new_stats[str(new_idx)] = stats[str(chosen)]
            used_old.add(chosen)
            moved += 1

    # Replace stats with re-aligned buckets, keep unmatched below cutoff
    stats.clear()
    stats.update(new_stats)

    # Now, reset any missing buckets beyond what we re-aligned
    for h in curr_hops:
        idx = str(int(h.get("count", 0)))
        stats.setdefault(idx, {"_order": [], "last": None, "wins": 0})

    if logger:
        logger.debug(f"[labels] realign_then_reset moved {moved} buckets; now {len(stats)} buckets total")

def _apply_reset_policy(stats, prev_hops, curr_hops, settings, logger=None):
    """
    Apply the reset/realign policy when the hop path changes.
    Controlled by settings['labels']['reset_mode'].
    """
    labels_cfg = (settings.get("labels") or {})
    mode = (labels_cfg.get("reset_mode") or "from_first_diff").strip().lower()

    first_diff = _first_diff_index(prev_hops, curr_hops)
    if first_diff is None:
        return  # nothing to do

    if logger:
        logger.debug(f"[labels] path changed; first differing hop = {first_diff}; mode={mode}")

    if mode == "none":
        return
    if mode == "all":
        stats.clear()
        if logger:
            logger.debug("[labels] reset mode = all; cleared all hop stats")
        return
    if mode == "realign_then_reset":
        _realign_then_reset(stats, prev_hops, curr_hops, logger=logger)
        return
    # default: from_first_diff
    _reset_stats_from(stats, first_diff, logger=logger)

# ---------------- Label decision ----------------
def _decide_label_per_hop(stats, hops_json_path, logger=None):
    """
    Build human-friendly labels per hop from current rolling stats.
    Also writes <ip>_hops.json for other modules/HTML.
    """
    labels = {}
    out = []
    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        items = [(k, s[k]) for k in s if isinstance(s.get(k), int) and k not in IGNORE_HOSTS]
        total = sum(c for _, c in items)
        if total == 0:
            continue
        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total
        hop_int = int(hop_str)

        if logger:
            logger.debug(
                f"[hop {hop_int}] label-calc total={total} top={top_host} "
                f"share={share:.2f} items={items[:TOPK_TO_SHOW]}"
            )

        if share < UNSTABLE_THRESHOLD and len(items) >= 2:
            sample = ", ".join(h for h, _ in items[:TOPK_TO_SHOW])
            host_label = f"varies ({sample})"
        else:
            host_label = s.get("last") or top_host

        labels[hop_int] = f"{hop_int}: {host_label}"
        out.append({"count": hop_int, "host": host_label})

    if out:
        with open(hops_json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
    return labels

# ---------------- Main loop ----------------
def monitor_target(ip, source_ip, settings, logger):
    # Paths/settings
    rrd_dir        = settings.get("rrd_directory", "rrd")
    log_directory  = settings.get("log_directory", "/tmp")
    interval       = settings.get("interval_seconds", 60)
    severity_rules = settings.get("log_severity_rules", [])

    rrd_path      = os.path.join(rrd_dir, f"{ip}.rrd")
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    # Ensure RRDs exist
    os.makedirs(rrd_dir, exist_ok=True)
    init_rrd(rrd_path, settings, logger)
    init_per_hop_rrds(ip, settings, logger)

    prev_hops = []
    prev_loss_state = {}

    logger.info(f"[{ip}] Monitoring loop started — running MTR snapshots")

    while True:
        hops = run_mtr(ip, source_ip, logger, settings=settings)
        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — target unreachable or command failed")
            time.sleep(interval)
            continue

        hop_path_changed = hops_changed(prev_hops, hops)

        curr_loss_state = {h.get("count"): round(h.get("Loss%", 0.0), 2)
                           for h in hops if h.get("Loss%", 0.0) > 0.0}
        loss_changed = (curr_loss_state != prev_loss_state)

        if hop_path_changed:
            diff = DeepDiff([h.get("host") for h in prev_hops],
                            [h.get("host") for h in hops],
                            ignore_order=False)
            context = {
                "hop_changed": True,
                "hop_added":   bool(diff.get("iterable_item_added")),
                "hop_removed": bool(diff.get("iterable_item_removed")),
            }
            for key, value in diff.get("values_changed", {}).items():
                hop_index = key.split("[")[-1].rstrip("]")
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, (level or "info").lower(), logger.info)
                msg = f"[{ip}] Hop {hop_index} changed from {value.get('old_value')} to {value.get('new_value')}"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        if loss_changed:
            for hop_num, loss in curr_loss_state.items():
                context = {
                    "loss": loss,
                    "prev_loss": prev_loss_state.get(hop_num, 0.0),
                    "hop_changed": hop_path_changed,
                }
                tag, level = evaluate_severity_rules(severity_rules, context)
                default_fn = logger.warning if loss > 0 else logger.info
                log_fn = getattr(logger, (level or ("WARNING" if loss > 0 else "INFO")).lower(), default_fn)
                msg = f"[{ip}] Loss at hop {hop_num}: {loss}% (prev: {context['prev_loss']}%)"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # ---- Update RRDs every iteration ----
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log)

        # ---- Labels: realign/reset BEFORE adding this snapshot ----
        stats_path, hops_json_path = _label_paths(ip, settings)
        stats = _load_stats(stats_path)

        if hop_path_changed:
            _apply_reset_policy(stats, prev_hops, hops, settings, logger=logger)

        # Now update stats with the current snapshot
        stats = _update_stats_with_snapshot(stats, hops)
        _save_stats(stats_path, stats)

        # Decide labels from the freshly updated stats, write <ip>_hops.json
        _decide_label_per_hop(stats, hops_json_path, logger)

        # Apply labels downstream (graphs/HTML read the updated file)
        update_hop_labels_only(ip, hops, settings, logger)

        if hop_path_changed or loss_changed:
            logger.debug(f"[{ip}] Parsed hops: {[ (h.get('count'), h.get('host'), h.get('Avg')) for h in hops ]}")
            save_trace_and_json(ip, hops, settings, logger)
            logger.info(f"[{ip}] Traceroute and hop map saved.")
        else:
            logger.debug(f"[{ip}] No change detected — {len(hops)} hops parsed. RRD still updated.")

        prev_hops = hops
        prev_loss_state = curr_loss_state
        time.sleep(interval)
