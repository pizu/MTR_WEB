#!/usr/bin/env python3
"""
modules/monitor.py

Monitors one target IP/host in a loop:
  1) Run MTR snapshot (via modules.mtr_runner.run_mtr)
  2) Detect hop-path changes + loss changes
  3) Update RRDs every iteration (per-hop + optional legacy multi-hop)
  4) Maintain rolling hop label stats (drives "varies (...)" labels)
  5) Save traceroute JSON when something changed
  6) Hot-reload mtr_script_settings.yaml (interval, rules, labels, etc.) without restart

This module expects to be invoked by mtr_watchdog.py:
    monitor_target(ip, source_ip, settings, logger)

Logging
-------
This function logs using the logger passed in by mtr_watchdog (usually named 'mtr_watchdog').
If you want a separate logger key for fine-grained YAML control, you can:
    child_logger = logger.getChild("monitor")  # and add logging_levels.mtr_watchdog.monitor in YAML
…but keeping a single per-target file simplifies operations.
"""

import os
import json
import time
from typing import Optional, Dict, Any, List
from datetime import datetime

from deepdiff import DeepDiff

from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, init_per_hop_rrds, update_rrd
from modules.severity import evaluate_severity_rules, hops_changed
from modules.trace_exporter import save_trace_and_json, update_hop_labels_only
from modules.utils import load_settings, refresh_logger_levels

# --- Default label tunables (YAML can override these at runtime) ---
UNSTABLE_THRESHOLD = 0.45   # top share < threshold and competition -> "varies (...)"
TOPK_TO_SHOW       = 3
MAJORITY_WINDOW    = 200
STICKY_MIN_WINS    = 3
IGNORE_HOSTS       = set()

# ---------------- YAML-driven label config helper ----------------
def _label_cfg(settings: dict) -> dict:
    """
    Pull label tuning knobs from settings['labels'], falling back to defaults.
    Read every loop so YAML edits take effect live.
    """
    labels = settings.get("labels") or {}
    return {
        "reset_mode":         str(labels.get("reset_mode", "from_first_diff")).strip().lower(),
        "unstable_threshold": float(labels.get("unstable_threshold", UNSTABLE_THRESHOLD)),
        "topk_to_show":       int(labels.get("topk_to_show",       TOPK_TO_SHOW)),
        "majority_window":    int(labels.get("majority_window",    MAJORITY_WINDOW)),
        "sticky_min_wins":    int(labels.get("sticky_min_wins",    STICKY_MIN_WINS)),
    }

# ---------------- Helpers for label/traceroute files ----------------
def _label_paths(ip: str, settings: dict):
    """
    Returns:
      stats_path:  traceroute/<ip>_hops_stats.json
      hops_json:   traceroute/<ip>_hops.json
    Ensures traceroute directory exists.
    """
    trace_dir = settings.get("traceroute_directory", "traceroute")
    os.makedirs(trace_dir, exist_ok=True)
    stem = os.path.join(trace_dir, ip)
    return stem + "_hops_stats.json", stem + "_hops.json"

def _load_stats(stats_path: str) -> dict:
    try:
        with open(stats_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_stats(stats_path: str, stats: dict) -> None:
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

# ---------------- Stats maintenance ----------------
def _update_stats_with_snapshot(
    stats: dict,
    hops: list,
    majority_window: int,
    sticky_min_wins: int,
    logger=None
) -> dict:
    """
    Update rolling counts keyed by hop index (string). Uses sticky modal logic
    and a soft decay window to avoid unbounded growth.

    IMPORTANT: ignores hop_count < 1 to prevent phantom hop0.
    """
    for h in hops:
        raw = h.get("count", 0)
        try:
            hop_num = int(raw)
        except (TypeError, ValueError):
            continue
        if hop_num < 1:
            if logger:
                logger.debug(f"[labels] skipping invalid hop count={raw} host={h.get('host')}")
            continue

        hop_idx = str(hop_num)
        host = h.get("host")
        if host is None:
            continue

        s = stats.setdefault(hop_idx, {"_order": [], "last": None, "wins": 0})
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)
        s[host] += 1

        # soft decay when window exceeded
        total_counts = sum(v for k, v in s.items() if isinstance(s.get(k), int))
        if total_counts > majority_window:
            for key in list(s["_order"])[::-1]:  # oldest first
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

        # sticky modal logic
        modal = max((k for k in s if isinstance(s.get(k), int)), key=lambda k: s[k], default=None)
        cur = s.get("last")
        if cur is None:
            s["last"] = modal
            s["wins"] = 1
        elif modal == cur:
            s["wins"] = min(s.get("wins", 0) + 1, sticky_min_wins)
        else:
            s["wins"] = s.get("wins", 0) - 1
            if s["wins"] <= 0:
                s["last"] = modal
                s["wins"] = 1

    return stats

# ---------------- Path-change stat hygiene ----------------
def _first_diff_index(prev_hops: list, curr_hops: list) -> Optional[int]:
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

def _reset_stats_from(stats: dict, start_hop_int: int, logger=None) -> None:
    """Drop stats for hop indices >= start_hop_int (1-based)."""
    to_del = [k for k in stats.keys() if k.isdigit() and int(k) >= start_hop_int]
    for k in to_del:
        stats.pop(k, None)
    if logger:
        logger.debug(f"[labels] reset stats from hop {start_hop_int} (inclusive); removed {len(to_del)} entries")

def _realign_then_reset(stats: dict, prev_hops: list, curr_hops: list, logger=None) -> None:
    """
    Best-effort migration:
      - Build map: last_modal_host -> old_hop_index
      - For each current hop, if its host matches a 'last' modal in the map,
        move that stats bucket to the new hop index.
      - Reset any leftover indexes that didn’t match.
    """
    modal_to_oldidx = {}
    for idx_str, s in list(stats.items()):
        last = s.get("last")
        if isinstance(last, str):
            modal_to_oldidx.setdefault(last, []).append(int(idx_str))

    moved = 0
    new_stats = {}
    used_old = set()

    for h in curr_hops:
        raw = h.get("count", 0)
        try:
            new_idx = int(raw)
        except (TypeError, ValueError):
            continue
        if new_idx < 1:
            continue
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

    stats.clear()
    stats.update(new_stats)

    for h in curr_hops:
        raw = h.get("count", 0)
        try:
            idx = int(raw)
        except (TypeError, ValueError):
            continue
        if idx < 1:
            continue
        stats.setdefault(str(idx), {"_order": [], "last": None, "wins": 0})

    if logger:
        logger.debug(f"[labels] realign_then_reset moved {moved} buckets; now {len(stats)} buckets total")

def _apply_reset_policy(stats: dict, prev_hops: list, curr_hops: list, reset_mode: str, logger=None) -> None:
    """
    Apply the reset/realign policy when the hop path changes.
    reset_mode: none | from_first_diff | realign_then_reset | all
    """
    first_diff = _first_diff_index(prev_hops, curr_hops)
    if first_diff is None:
        return  # nothing to do

    if logger:
        logger.debug(f"[labels] path changed; first differing hop = {first_diff}; mode={reset_mode}")

    if reset_mode == "none":
        return
    if reset_mode == "all":
        stats.clear()
        if logger:
            logger.debug("[labels] reset mode = all; cleared all hop stats")
        return
    if reset_mode == "realign_then_reset":
        _realign_then_reset(stats, prev_hops, curr_hops, logger=logger)
        return
    # default: from_first_diff
    _reset_stats_from(stats, first_diff, logger=logger)

# ---------------- Label decision ----------------
def _decide_label_per_hop(
    stats: dict,
    hops_json_path: str,
    unstable_threshold: float,
    topk_to_show: int,
    logger=None
) -> dict:
    """
    Build human-friendly labels per hop from current rolling stats.
    Also writes <ip>_hops.json for other modules/HTML.
    """
    labels = {}
    out = []
    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        hop_int = int(hop_str)
        if hop_int < 1:
            # ignore any legacy "0" keys that might still be on disk
            continue

        items = [(k, s[k]) for k in s if isinstance(s.get(k), int) and k not in IGNORE_HOSTS]
        total = sum(c for _, c in items)
        if total == 0:
            continue
        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total

        if logger:
            logger.debug(
                f"[hop {hop_int}] label-calc total={total} top={top_host} "
                f"share={share:.2f} items={items[:topk_to_show]}"
            )

        if share < unstable_threshold and len(items) >= 2:
            sample = ", ".join(h for h, _ in items[:topk_to_show])
            host_label = f"varies ({sample})"
        else:
            host_label = s.get("last") or top_host

        labels[hop_int] = f"{hop_int}: {host_label}"
        out.append({"count": hop_int, "host": host_label})

    if out:
        with open(hops_json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
    return labels

# ---------------- Settings hot-reload support ----------------
def _resolve_settings_path(settings: dict) -> str:
    """
    Find the path to mtr_script_settings.yaml so we can hot-reload it.
    Priority:
      1) settings["_settings_path"] if supplied by caller (watchdog can inject this)
      2) repo_root/mtr_script_settings.yaml
    """
    # Try injected path first
    injected = settings.get("_settings_path")
    if isinstance(injected, str) and os.path.isfile(injected):
        return injected

    # Fallback: compute repo root relative to this file (modules/ -> scripts/ -> repo)
    modules_dir = os.path.abspath(os.path.dirname(__file__))         # .../scripts/modules
    scripts_dir = os.path.abspath(os.path.join(modules_dir, os.pardir))
    repo_root   = os.path.abspath(os.path.join(scripts_dir, os.pardir))
    return os.path.join(repo_root, "mtr_script_settings.yaml")

def _safe_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

# ---------------- Main monitor entrypoint ----------------
def monitor_target(ip, source_ip, settings, logger):
    """
    Monitor loop for a single target.
    - ip:        destination IP/host
    - source_ip: optional source IP (passed to MTR)
    - settings:  dict loaded from YAML (will be hot-reloaded)
    - logger:    logging.Logger provided by watchdog
    """

    # --- Resolve settings file for hot-reload and track mtime ---
    SETTINGS_FILE = _resolve_settings_path(settings)
    last_settings_mtime = _safe_mtime(SETTINGS_FILE)

    # --- Initial settings consumption ---
    # Align default to rrd_handler: default dir is "data" (not "rrd")
    rrd_dir        = settings.get("rrd_directory", "data")
    log_directory  = settings.get("log_directory", "/tmp")
    interval       = int(settings.get("interval_seconds", 60))
    severity_rules = settings.get("log_severity_rules", [])

    # Legacy single-file (multi-hop) path + per-hop debug log
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path      = os.path.join(rrd_dir, f"{ip}.rrd")
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    # Ensure RRD files exist (legacy + optional pre-create per-hop)
    init_rrd(rrd_path, settings, logger)
    init_per_hop_rrds(ip, settings, logger)

    prev_hops: List[Dict[str, Any]] = []
    prev_loss_state: Dict[int, float] = {}

    logger.info(f"[{ip}] Monitoring loop started — running MTR snapshots (interval={interval}s)")

    while True:
        # --- Hot reload of settings on file change (updates interval, rules, label knobs, and logger levels) ---
        curr_mtime = _safe_mtime(SETTINGS_FILE)
        if curr_mtime != last_settings_mtime and curr_mtime > 0:
            try:
                settings = load_settings(SETTINGS_FILE)
                # Re-apply level from YAML to this existing logger name used by watchdog
                refresh_logger_levels(logger, "mtr_watchdog", settings)
                # Re-consume settings used locally
                rrd_dir        = settings.get("rrd_directory", "data")
                log_directory  = settings.get("log_directory", "/tmp")
                interval       = int(settings.get("interval_seconds", interval))
                severity_rules = settings.get("log_severity_rules", severity_rules)
                # Ensure paths still valid after edits
                os.makedirs(rrd_dir, exist_ok=True)
                rrd_path      = os.path.join(rrd_dir, f"{ip}.rrd")
                debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")
                last_settings_mtime = curr_mtime
                logger.info(f"[{ip}] Settings reloaded. interval={interval}s, rrd_dir={rrd_dir}")
            except Exception as e:
                logger.error(f"[{ip}] Failed to hot-reload settings: {e}")

        # --- Run one MTR snapshot ---
        hops = run_mtr(ip, source_ip, logger, settings=settings)
        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — target unreachable or command failed")
            time.sleep(interval)
            continue

        hop_path_changed = hops_changed(prev_hops, hops)

        # --- Pull label tuning from YAML each loop (overrides module defaults) ---
        label_knobs = _label_cfg(settings)
        unstable_threshold = label_knobs["unstable_threshold"]
        topk_to_show       = label_knobs["topk_to_show"]
        majority_window    = label_knobs["majority_window"]
        sticky_min_wins    = label_knobs["sticky_min_wins"]
        reset_mode         = label_knobs["reset_mode"]
        logger.debug(
            f"[labels cfg] reset_mode={reset_mode} "
            f"unstable_threshold={unstable_threshold} "
            f"topk_to_show={topk_to_show} "
            f"majority_window={majority_window} "
            f"sticky_min_wins={sticky_min_wins}"
        )

        # --- Loss tracking per hop (ignore hop < 1) ---
        curr_loss_state: Dict[int, float] = {}
        for h in hops:
            try:
                hop_num = int(h.get("count", 0))
            except (TypeError, ValueError):
                continue
            if hop_num < 1:
                continue
            loss = h.get("Loss%", 0.0)
            if loss is not None:
                try:
                    lf = float(loss)
                except (TypeError, ValueError):
                    lf = 0.0
                if lf > 0.0:
                    curr_loss_state[hop_num] = round(lf, 2)

        loss_changed = (curr_loss_state != prev_loss_state)

        # --- Hop path change logging with severity evaluation ---
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

        # --- Loss change logging with severity evaluation ---
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
        # (Per-hop + optional legacy multi-hop update. Also writes optional debug text log.)
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log, logger=logger)

        # ---- Labels: realign/reset BEFORE adding this snapshot ----
        stats_path, hops_json_path = _label_paths(ip, settings)
        stats = _load_stats(stats_path)

        if hop_path_changed:
            _apply_reset_policy(stats, prev_hops, hops, reset_mode, logger=logger)

        # Now update stats with the current snapshot (hop>=1 only)
        stats = _update_stats_with_snapshot(
            stats,
            hops,
            majority_window=majority_window,
            sticky_min_wins=sticky_min_wins,
            logger=logger
        )
        _save_stats(stats_path, stats)

        # Decide labels from the freshly updated stats, write <ip>_hops.json
        _decide_label_per_hop(
            stats,
            hops_json_path,
            unstable_threshold=unstable_threshold,
            topk_to_show=topk_to_show,
            logger=logger
        )

        # Apply labels downstream (reads the fresh file; function is idempotent)
        update_hop_labels_only(ip, hops, settings, logger)

        if hop_path_changed or loss_changed:
            logger.debug(f"[{ip}] Parsed hops: {[ (h.get('count'), h.get('host'), h.get('Avg')) for h in hops ]}")
            save_trace_and_json(ip, hops, settings, logger)
            logger.info(f"[{ip}] Traceroute and hop map saved.")
        else:
            logger.debug(f"[{ip}] No change detected — {len(hops)} hops parsed. RRD still updated.")

        # --- keep last snapshot & sleep until next cycle ---
        prev_hops = hops
        prev_loss_state = curr_loss_state
        time.sleep(interval)
