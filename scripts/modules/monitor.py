#!/usr/bin/env python3
"""
modules/monitor.py

WHAT THIS FILE DOES (high-level):
---------------------------------
For ONE target IP/host, it loops forever and:
  1) Runs a single MTR snapshot (via modules.mtr_runner.run_mtr).
  2) Detects hop-path changes and packet loss changes.
  3) Updates the main RRD (aggregate) and the per-hop RRDs EVERY iteration.
  4) Builds/updates hop label stats and decides per-hop labels (e.g., "varies (...)" vs a stable host).
  5) Saves traceroute + per-hop map when something changed (to reduce noise).
  6) Sleeps and repeats.

KEY FIXES in this version:
--------------------------
- `_decide_label_per_hop(...)`:
    * No more accidental self-recursion.
    * Accepts `logger` (optional) for useful debug lines.
- ORDER of operations:
    * We now compute & write the hop labels JSON BEFORE calling
      `update_hop_labels_only(...)`, so the latest labels are used immediately.

PREREQUISITES:
--------------
- Other modules in scripts/modules:
    - mtr_runner.run_mtr(...)
    - rrd_handler.init_rrd(), init_per_hop_rrds(), update_rrd()
    - trace_exporter.save_trace_and_json(), update_hop_labels_only()
    - severity.evaluate_severity_rules(), severity.hops_changed()
- YAML settings are already loaded by the watchdog and passed as `settings` here.
"""

import os
import json
import time
from deepdiff import DeepDiff  # used to compute human-friendly hop path diffs

# Project modules (import paths assume this file is under scripts/modules/)
from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, init_per_hop_rrds, update_rrd
from modules.severity import evaluate_severity_rules, hops_changed
from modules.trace_exporter import save_trace_and_json, update_hop_labels_only

# ---- Tunables for label decisions (fine-tune to taste) ----------------------
UNSTABLE_THRESHOLD = 0.45   # if the top host's share < 45% AND there is competition -> label "varies (...)"
TOPK_TO_SHOW       = 3      # when labeling "varies", show up to this many example hosts
MAJORITY_WINDOW    = 200    # rolling window size for counts per hop (prevents unbounded growth)
STICKY_MIN_WINS    = 3      # hysteresis: how many consecutive "wins" before we accept a new modal host
IGNORE_HOSTS       = set()  # e.g., add "_gateway" to exclude it from the vote; leave empty to include everything (incl. "???")

# ---- Small helpers for file paths and JSON IO --------------------------------
def _label_paths(ip: str, settings: dict):
    """
    Decide where we store:
      - <ip>_hops_stats.json : rolling counts per hop per host
      - <ip>_hops.json       : the current per-hop label list (what the HTML/graph readers use)
    """
    trace_dir = settings.get("traceroute_directory", "traceroute")
    os.makedirs(trace_dir, exist_ok=True)
    stem = os.path.join(trace_dir, ip)
    return stem + "_hops_stats.json", stem + "_hops.json"

def _load_stats(stats_path: str) -> dict:
    """Load existing rolling counts (if any). Returns {} if file missing/bad."""
    try:
        with open(stats_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_stats(stats_path: str, stats: dict) -> None:
    """Write rolling counts back to disk (pretty JSON)."""
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

# ---- Stats maintenance -------------------------------------------------------
def _update_stats_with_snapshot(stats: dict, hops: list) -> dict:
    """
    Update the rolling counts for each hop using the latest MTR snapshot.

    'stats' structure (per hop index as string):
      stats[hop_idx_str] = {
        "_order": [most_recent_host, ...],  # to help decay oldest entries first
        "last":   "<current_modal_host>",
        "wins":   <consecutive_wins_for_last>,
        "<hostA>": <count>,
        "<hostB>": <count>,
        ...
      }
    """
    for h in hops:
        hop_idx = str(int(h.get("count", 0)))
        host = h.get("host")  # may be "???"
        if host is None:
            continue

        s = stats.setdefault(hop_idx, {"_order": [], "last": None, "wins": 0})

        # Initialize count for a new host at this hop; record it newest-first
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)

        # Increment that host's count
        s[host] += 1

        # Soft decay: if the sum of counts is above our window, reduce the oldest hosts first
        total_counts = sum(v for k, v in s.items() if isinstance(s.get(k), int))
        if total_counts > MAJORITY_WINDOW:
            for key in list(s["_order"])[::-1]:  # iterate from oldest to newest
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        # remove the host entirely when it decays to zero
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

        # "Sticky" modal host logic to avoid flip-flopping on small sample changes
        # Determine the host with the highest count (modal)
        modal = max((k for k in s if isinstance(s.get(k), int)), key=lambda k: s[k], default=None)
        cur = s.get("last")
        if cur is None:
            s["last"] = modal
            s["wins"] = 1
        elif modal == cur:
            s["wins"] = min(s.get("wins", 0) + 1, STICKY_MIN_WINS)
        else:
            # If a *different* modal appears, we require a few consecutive "wins" before switching.
            s["wins"] = s.get("wins", 0) - 1
            if s["wins"] <= 0:
                s["last"] = modal
                s["wins"] = 1

    return stats

# ---- Label decision (this powers the "varies (...)" behavior) ----------------
def _decide_label_per_hop(stats: dict, hops_json_path: str, logger=None) -> dict:
    """
    Decide per-hop labels based on rolling counts in 'stats'.

    Returns a dict:
      { hop_index(int): "hop_index: <label>" }

    ALSO writes <ip>_hops.json (compact list [{"count": i, "host": label}, ...])
    which downstream helpers/HTML use.
    """
    labels: dict[int, str] = {}
    out_list = []

    # Iterate in hop order (keys in 'stats' are strings, so we sort by int value)
    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        # Build a (host, count) list; ignore non-int entries like _order/last/wins
        items = [(k, s[k]) for k in s if isinstance(s.get(k), int) and k not in IGNORE_HOSTS]
        total = sum(c for _, c in items)
        if total == 0:
            continue

        # Highest count first
        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total
        hop_int = int(hop_str)

        # Optional debug: see the ingredients of the decision
        if logger:
            logger.debug(
                f"[hop {hop_int}] label-calc total={total} top={top_host} "
                f"share={share:.2f} items={items[:TOPK_TO_SHOW]}"
            )

        # If the top host is not dominant and there is competition, mark as "varies"
        if share < UNSTABLE_THRESHOLD and len(items) >= 2:
            sample = ", ".join(h for h, _ in items[:TOPK_TO_SHOW])
            host_label = f"varies ({sample})"
        else:
            # Otherwise use the sticky "last" modal if present, else the current modal
            host_label = s.get("last") or top_host

        labels[hop_int] = f"{hop_int}: {host_label}"
        out_list.append({"count": hop_int, "host": host_label})

    # Persist the compact labels JSON for downstream readers
    if out_list:
        with open(hops_json_path, "w", encoding="utf-8") as f:
            json.dump(out_list, f, indent=2)

    return labels

# ---- Main loop ---------------------------------------------------------------
def monitor_target(ip: str, source_ip: str | None, settings: dict, logger) -> None:
    """
    Long-running monitor loop for a single target.

    Args:
      ip         : destination IP/hostname to monitor
      source_ip  : optional source address to bind for MTR
      settings   : dict with project settings (already loaded by the watchdog)
      logger     : shared project logger (writes to central + per-target logs)
    """

    # --- Resolve key paths / settings (RRD, logging, interval, rules) ---
    rrd_dir        = settings.get("rrd_directory", "rrd")
    log_directory  = settings.get("log_directory", "/tmp")
    interval       = settings.get("interval_seconds", 60)           # sleep between iterations
    severity_rules = settings.get("log_severity_rules", [])         # optional severity tagging rules

    # Path to the MAIN RRD file (aggregate, multi-DS). Per-hop RRDs are handled separately.
    rrd_path      = os.path.join(rrd_dir, f"{ip}.rrd")
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    # Ensure directories/rrds exist BEFORE loop begins
    os.makedirs(rrd_dir, exist_ok=True)
    init_rrd(rrd_path, settings, logger)        # create/verify the main RRD
    init_per_hop_rrds(ip, settings, logger)     # create/verify per-hop RRDs

    # Keep last-state to detect changes
    prev_hops       : list = []   # previous hop list
    prev_loss_state : dict = {}   # hop_index -> loss%

    logger.info(f"[{ip}] Monitoring loop started — running MTR snapshots")

    # =======================
    # Continuous monitoring
    # =======================
    while True:
        # 1) Capture ONE MTR snapshot. Returns a list of hop dicts, or [] on failure.
        hops = run_mtr(ip, source_ip, logger, settings=settings)

        # If no data (timeout/unreachable/parse error), log and retry next cycle.
        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — target unreachable or command failed")
            time.sleep(interval)
            continue

        # 2) Change detection
        # (a) Hop path changes (values_changed, items added/removed, order matters)
        hop_path_changed = hops_changed(prev_hops, hops)

        # (b) Loss changes (we track only hops with >0% loss for compactness)
        curr_loss_state = {
            h.get("count"): round(h.get("Loss%", 0.0), 2)
            for h in hops if h.get("Loss%", 0.0) > 0.0
        }
        loss_changed = (curr_loss_state != prev_loss_state)

        # 3) If hop path changed, compute and log a human-readable diff
        if hop_path_changed:
            diff = DeepDiff(
                [h.get("host") for h in prev_hops],
                [h.get("host") for h in hops],
                ignore_order=False
            )
            context = {
                "hop_changed": True,
                "hop_added":   bool(diff.get("iterable_item_added")),
                "hop_removed": bool(diff.get("iterable_item_removed")),
            }
            for key, value in diff.get("values_changed", {}).items():
                # key like 'root[2]' -> extract index "2"
                hop_index = key.split("[")[-1].rstrip("]")
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, (level or "info").lower(), logger.info)
                msg = f"[{ip}] Hop {hop_index} changed from {value.get('old_value')} to {value.get('new_value')}"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # 4) If loss state changed, log differences per hop
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

        # 5) Update RRDs EVERY iteration (graphs stay fresh even without changes)
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log)

        # 5a) Compute & SAVE per-hop labels ("varies (...)" logic) BEFORE applying them
        #     This ensures the helper that reads <ip>_hops.json sees the latest labels.
        stats_path, hops_json_path = _label_paths(ip, settings)
        stats = _load_stats(stats_path)
        stats = _update_stats_with_snapshot(stats, hops)
        _save_stats(stats_path, stats)
        labels = _decide_label_per_hop(stats, hops_json_path, logger)  # writes <ip>_hops.json

        # 5b) Now apply those labels (downstream modules/HTML use the fresh file)
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
        time.sleep(interval)
