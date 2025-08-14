#!/usr/bin/env python3
"""
monitor.py

Watches a single target IP/host in a loop:
- runs one MTR cycle per iteration (via run_mtr)
- detects hop-path changes and loss changes
- updates the main and per-hop RRDs every iteration
- saves traceroute + JSON when something changes
- logs with severity based on simple rules

Note: this version accepts a static `settings` dict. If you want live reload of
YAML each cycle, pass a settings *path* and call `load_settings(...)` inside the loop.
"""

import os               # filesystem paths (RRD locations, etc.)
import json
import time             # sleeping between iterations
from deepdiff import DeepDiff  # diff previous vs current hop path lists

# Import modular functions (keeps this file focused on orchestration)
from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, init_per_hop_rrds, update_rrd
from modules.trace_exporter import save_trace_and_json
from modules.severity import evaluate_severity_rules, hops_changed
from modules.trace_exporter import save_trace_and_json, update_hop_labels_only

UNSTABLE_THRESHOLD = 0.45   # top host <45% -> label as "varies (...)"
TOPK_TO_SHOW       = 3      # show up to 3 examples
MAJORITY_WINDOW    = 200    # soft cap on samples kept per hop
STICKY_MIN_WINS    = 3      # hysteresis to avoid flip-flop
IGNORE_HOSTS       = set()  # keep ???; add "_gateway" here if you want to ignore it

def _label_paths(ip, settings):
    trace_dir = settings.get("traceroute_directory", "traceroute")
    os.makedirs(trace_dir, exist_ok=True)
    stem = os.path.join(trace_dir, ip)
    return stem + "_hops_stats.json", stem + "_hops.json"

def _load_stats(stats_path):
    try:
        return json.loads(open(stats_path, encoding="utf-8").read())
    except Exception:
        return {}

def _save_stats(stats_path, stats):
    open(stats_path, "w", encoding="utf-8").write(json.dumps(stats, indent=2))

def _update_stats_with_snapshot(stats, hops):
    # hops: list of dicts from run_mtr(); we keep host counts per hop index
    for h in hops:
        hop = str(int(h.get("count", 0)))
        host = h.get("host")
        if host is None:  # allow '???'
            continue
        s = stats.setdefault(hop, {"_order": [], "last": None, "wins": 0})
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)
        s[host] += 1
        # decay to stay within window
        total = sum(v for k, v in s.items() if isinstance(v, int))
        if total > MAJORITY_WINDOW:
            for key in list(s["_order"])[::-1]:
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break
        # sticky
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

def _decide_label_per_hop(stats, hops_json_path, logger=None):
    #labels = {}
    labels = _decide_label_per_hop(stats, hops_json_path, logger)
    out = []
    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        # collect counts incl. '???' (we do NOT filter it out)
        items = [(k, s[k]) for k in s if isinstance(s.get(k), int) and k not in IGNORE_HOSTS]
        total = sum(c for _, c in items)
        if total == 0:
            continue
        items.sort(key=lambda kv: -kv[1])
        if total > 0 and logger:
            logger.debug(
                f"[{hop_int}] label-calc total={total} top={top_host} "
                f"share={share:.2f} items={items[:TOPK_TO_SHOW]}"
            )

        top_host, top_count = items[0]
        share = top_count / total
        if share < UNSTABLE_THRESHOLD and len(items) >= 2:
            sample = ", ".join(h for h, _ in items[:TOPK_TO_SHOW])
            host_label = f"varies ({sample})"
        else:
            host_label = s.get("last") or top_host
        hop_int = int(hop_str)
        labels[hop_int] = f"{hop_int}: {host_label}"
        out.append({"count": hop_int, "host": host_label})
    if out:
        open(hops_json_path, "w", encoding="utf-8").write(json.dumps(out, indent=2))
    return labels

def monitor_target(ip, source_ip, settings, logger):
    """
    Main monitoring function for a single target.

    Args:
        ip (str): target IP/hostname to monitor.
        source_ip (str|None): optional source address for MTR (passed through).
        settings (dict): global settings loaded from YAML.
        logger (logging.Logger): project logger instance.
    """

    # -------------------------------
    # Resolve paths and core settings
    # -------------------------------
    rrd_dir = settings.get("rrd_directory", "rrd")         # where the .rrd files live
    log_directory = settings.get("log_directory", "/tmp")  # where debug logs go
    interval = settings.get("interval_seconds", 60)         # seconds between cycles
    severity_rules = settings.get("log_severity_rules", [])  # optional rules for tagging

    # Main RRD file holds aggregated hops (multi-DS). Per-hop RRDs are separate.
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    # Ensure destination directories/rrds exist before entering the loop
    os.makedirs(rrd_dir, exist_ok=True)         # create RRD dir if missing
    init_rrd(rrd_path, settings, logger)        # create/validate the main RRD
    init_per_hop_rrds(ip, settings, logger)     # create/validate per-hop RRDs

    # Keep last iteration's state so we can detect changes
    prev_hops = []            # previous hop list (for path change diffing)
    prev_loss_state = {}      # map: hop_index -> loss% (last seen)

    logger.info(f"[{ip}] Monitoring loop started — running MTR")

    # -------------------
    # Continuous run loop
    # -------------------
    while True:
        # 1) Execute a single MTR cycle and get a normalized list of hops
        hops = run_mtr(ip, source_ip, logger, settings=settings)

        # If the runner failed (timeout, unreachable, parsing error), wait and retry
        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — target unreachable or command failed")
            time.sleep(interval)
            continue

        # 2) Change detection
        # ------------------
        # (a) Hop path changes: did any hop host label change position/value?
        hop_path_changed = hops_changed(prev_hops, hops)

        # (b) Loss changes: build current loss map for hops with >0% loss
        curr_loss_state = {
            h.get("count"): round(h.get("Loss%", 0), 2)
            for h in hops if h.get("Loss%", 0) > 0
        }
        loss_changed = curr_loss_state != prev_loss_state

        # 3) If hop path changed, compute and log a human-readable diff
        if hop_path_changed:
            diff = DeepDiff(
                [h.get("host") for h in prev_hops],  # previous hop labels ordered
                [h.get("host") for h in hops],       # current hop labels ordered
                ignore_order=False                    # order matters in traceroute
            )
            # Build a context dict for severity rules (you can extend this over time)
            context = {
                "hop_changed": True,
                "hop_added": bool(diff.get("iterable_item_added")),
                "hop_removed": bool(diff.get("iterable_item_removed")),
            }
            # For each value change (e.g., hop[2] changed from A to B), log with a tag/level
            for key, value in diff.get("values_changed", {}).items():
                hop_index = key.split("[")[-1].rstrip("]")  # extract index from e.g. 'root[2]'
                tag, level = evaluate_severity_rules(severity_rules, context)
                # Choose the appropriate logger method; default to info if level missing
                log_fn = getattr(logger, level.lower(), logger.info) if tag and isinstance(level, str) else logger.info
                msg = f"[{ip}] Hop {hop_index} changed from {value.get('old_value')} to {value.get('new_value')}"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # 4) If loss state changed, log each hop's current loss with previous reference
        if loss_changed:
            for hop_num, loss in curr_loss_state.items():
                context = {
                    "loss": loss,
                    "prev_loss": prev_loss_state.get(hop_num, 0),
                    "hop_changed": hop_path_changed,
                }
                tag, level = evaluate_severity_rules(severity_rules, context)
                # Default to warning when there's loss; info when cleared/zero
                default_fn = logger.warning if loss > 0 else logger.info
                log_fn = getattr(logger, level.lower(), default_fn) if isinstance(level, str) else default_fn
                msg = f"[{ip}] Loss at hop {hop_num}: {loss}% (prev: {context['prev_loss']}%)"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # 5) Update RRDs every iteration (even if nothing changed)
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log)
        # 5a) Compute & save per-hop labels ("varies (...)" logic) BEFORE applying them
        stats_path, hops_json_path = _label_paths(ip, settings)
        stats = _load_stats(stats_path)
        stats = _update_stats_with_snapshot(stats, hops)
        _save_stats(stats_path, stats)
        labels = _decide_label_per_hop(stats, hops_json_path)  # writes <ip>_hops.json

        # 5b) Now apply those labels (RRD/HTML helpers can read the fresh file)
        update_hop_labels_only(ip, hops, settings, logger)

        # 6) Persist traceroute + hop map only when something changed (noise control)
        if hop_path_changed or loss_changed:
            logger.debug(f"[{ip}] Parsed hops: {[ (h.get('count'), h.get('host'), h.get('Avg')) for h in hops ]}")
            save_trace_and_json(ip, hops, settings, logger)
            logger.info(f"[{ip}] Traceroute and hop map saved.")
        else:
            logger.debug(f"[{ip}] No change detected — {len(hops)} hops parsed. RRD still updated.")

        # 7) Prepare for next cycle and sleep
        prev_hops = hops
        prev_loss_state = curr_loss_state
        time.sleep(interval)  # sleep the configured interval and repeat
