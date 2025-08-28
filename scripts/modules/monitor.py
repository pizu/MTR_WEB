#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/monitor.py
==================

Purpose
-------
Continuously monitor a single target (IP/Host) with MTR snapshots, detect
hop-path/loss changes, maintain per-hop label statistics (to decide when a hop
"varies"), and keep the per-target multi-hop RRD updated.

This file is designed to be **drop-in** for the MTR_WEB project and to obey
settings from `mtr_script_settings.yaml` (hot-reloaded at runtime).

What this module does each loop
-------------------------------
1) Run a single MTR snapshot for the target (via modules.mtr_runner.run_mtr).
2) Compare the hop path to the previous snapshot (via modules.severity.hops_changed)
   and log any changes with severity tagging (via modules.severity.evaluate_severity_rules).
3) Track per-hop packet loss changes and log them with severity.
4) Update the per-target multi-hop RRD file with hop metrics (via modules.rrd_handler.update_rrd).
5) Maintain rolling label statistics (per hop) so legends can show either a stable host
   or a "varies (a, b, c)" sample. This is persisted under settings.paths.traceroute.
6) Persist traceroute artifacts (strictly under YAML settings.paths.traceroute)
   using modules.graph_utils.save_trace_and_json and update_hop_labels_only.
7) Hot-reload mtr_script_settings.yaml on the fly (interval, label knobs, severity rules,
   logging levels, and path resolutions all refresh without restarting the process).

Key choices
-----------
- STRICT traceroute path policy: this module writes traceroute artifacts ONLY
  under settings['paths']['traceroute']; if that directory is missing, we fail fast.
- Labels reflect exactly the tokens seen under the MTR "Host" column:
  IPs, DNS names, and the literal "???" for no reply (or "(waiting for reply)" if we had
  an empty placeholder). We never allow bookkeeping keys ("wins", "last", "_order")
  to leak into the user-visible labels.

YAML knobs in mtr_script_settings.yaml
--------------------------------------
labels:
  reset_mode: "from_first_diff"   # none | from_first_diff | realign_then_reset | all
  unstable_threshold: 0.45        # if top share < threshold (and there is competition) => varies(...)
  topk_to_show: 3                 # in "varies (a, b, c)" show up to this many tokens (deduplicated)
  majority_window: 200            # sliding window size for counts (simple decay)
  sticky_min_wins: 3              # hysteresis for the "last" modal token

interval_seconds: 60              # MTR loop interval (seconds)
log_severity_rules: []            # optional list of rules (see modules.severity)

paths:
  rrd:        /opt/scripts/MTR_WEB/data
  html:       /opt/scripts/MTR_WEB/html
  graphs:     /opt/scripts/MTR_WEB/html/graphs
  logs:       /opt/scripts/MTR_WEB/logs
  traceroute: /opt/scripts/MTR_WEB/traceroute

Logging levels are controlled centrally by settings['logging_levels'] and are
applied/updated live by modules.utils.refresh_logger_levels().

Dependencies
------------
- modules.mtr_runner.run_mtr
- modules.rrd_handler.init_rrd / update_rrd
- modules.severity.evaluate_severity_rules / hops_changed
- modules.graph_utils.save_trace_and_json / update_hop_labels_only
- modules.utils.load_settings / resolve_all_paths / setup_logger / refresh_logger_levels

"""

from __future__ import annotations

import os
import json
import time
from typing import Optional, Dict, Any, List

from deepdiff import DeepDiff

from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, update_rrd
from modules.severity import evaluate_severity_rules, hops_changed
from modules.graph_utils import save_trace_and_json, update_hop_labels_only
from modules.utils import load_settings, resolve_all_paths, setup_logger, refresh_logger_levels

# =============================================================================
# Label tunables (defaults; all can be overridden by YAML at runtime)
# =============================================================================

UNSTABLE_THRESHOLD = 0.45   # if top share < threshold and competition exists => "varies (...)"
TOPK_TO_SHOW       = 3
MAJORITY_WINDOW    = 200
STICKY_MIN_WINS    = 3
IGNORE_HOSTS       = set()  # optional: add tokens to ignore in label tallies


# =============================================================================
# Helpers: YAML-driven label config, settings paths, timestamps
# =============================================================================

def _label_cfg(settings: dict) -> dict:
    """Return label-related knobs merged from YAML (with sane defaults)."""
    labels = settings.get("labels") or {}
    return {
        "reset_mode":         str(labels.get("reset_mode", "from_first_diff")).strip().lower(),
        "unstable_threshold": float(labels.get("unstable_threshold", UNSTABLE_THRESHOLD)),
        "topk_to_show":       int(labels.get("topk_to_show",       TOPK_TO_SHOW)),
        "majority_window":    int(labels.get("majority_window",    MAJORITY_WINDOW)),
        "sticky_min_wins":    int(labels.get("sticky_min_wins",    STICKY_MIN_WINS)),
    }


def _settings_path_from_settings(settings: dict) -> Optional[str]:
    """
    Locate the live settings file path so we can hot-reload:
      1) settings['_meta']['settings_path'] (preferred)
      2) repo-root/mtr_script_settings.yaml (fallback if present)
    """
    meta = settings.get("_meta") or {}
    sp = meta.get("settings_path")
    if isinstance(sp, str) and os.path.isfile(sp):
        return sp
    # Fallback: assume repo root has mtr_script_settings.yaml
    modules_dir = os.path.abspath(os.path.dirname(__file__))           # .../scripts/modules
    scripts_dir = os.path.abspath(os.path.join(modules_dir, os.pardir))
    repo_root   = os.path.abspath(os.path.join(scripts_dir, os.pardir))
    fallback = os.path.join(repo_root, "mtr_script_settings.yaml")
    return fallback if os.path.isfile(fallback) else None


def _safe_mtime(path: Optional[str]) -> float:
    """Return the file mtime or 0.0 on any error (used for hot-reload)."""
    if not path:
        return 0.0
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


# =============================================================================
# Host token normalization (match what the MTR "Host" column shows)
# =============================================================================

WAITING = "(waiting for reply)"  # used when host is empty/None/"*" / "?"
CONTROL_KEYS = {"wins", "last", "_order"}

def normalize_host_label(host) -> str:
    """
    Map raw MTR 'host' values to the exact token we want to display and tally.
    - Keep IPs, DNS names, and the literal '???' exactly as emitted by MTR JSON.
    - If host is None/empty/'*'/'?' → show '(waiting for reply)'.
    Note: your MTR JSON uses "???" as the unresolved token; we keep that verbatim.
    """
    if host is None:
        return WAITING
    h = str(host).strip()
    if not h or h in {"*", "?"}:
        return WAITING
    return h  # keep IPs, DNS names, and literal "???"


# =============================================================================
# Traceroute/label files (STRICTLY under YAML paths.traceroute)
# =============================================================================

def _label_paths(ip: str, settings: dict) -> tuple[str, str]:
    """
    Return (stats_path, hops_json_path) under the STRICT traceroute directory.

    Writers must refuse to write if settings.paths.traceroute is missing.
    """
    tr_dir = resolve_all_paths(settings).get("traceroute")
    if not tr_dir:
        raise RuntimeError("settings.paths.traceroute is missing")
    os.makedirs(tr_dir, exist_ok=True)
    stem = os.path.join(tr_dir, ip)
    return stem + "_hops_stats.json", stem + "_hops.json"

def _load_stats(stats_path: str) -> dict:
    """Load per-hop label stats; return {} on any error."""
    try:
        with open(stats_path, encoding="utf-8") as f:
            raw = json.load(f) or {}
    except Exception:
        return {}
    # Sanitize on read
    return _strip_control_keys(raw)

def _strip_control_keys(d: dict) -> dict:
    # Remove bookkeeping keys per hop bucket
    cleaned = {}
    for hop, data in (d or {}).items():
        if not isinstance(data, dict):
            continue
        cleaned[hop] = {k: v for k, v in data.items() if k not in CONTROL_KEYS}
    return cleaned

def _save_stats(stats_path: str, stats: dict) -> None:
    """
    Save per-hop stats to disk, stripping bookkeeping keys
    so only actual host tokens are persisted.
    """
    cleaned = _strip_control_keys(stats or {})
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2)

# =============================================================================
# Stats maintenance (rolling counts, decay, sticky modal logic)
# =============================================================================

def _update_stats_with_snapshot(
    stats: dict,
    hops: list,
    majority_window: int,
    sticky_min_wins: int,
    logger=None
) -> dict:
    """
    Update per-hop statistics with the current snapshot.
    We count how many times each token has appeared per hop index, apply a simple
    decay once the bucket exceeds majority_window, and maintain a "sticky" modal
    ('last' + 'wins') to stabilize labels when the top token fluctuates.

    Expected hop shape (from run_mtr JSON):
      h = {"count": <hop_index>, "host": <token>, "Loss%": <float>, ...}
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

        # Normalize the host token to match exactly what we want to display/tally.
        host = normalize_host_label(h.get("host"))
        if host is None:
            continue

        # Initialize hop bucket shape.
        s = stats.setdefault(hop_idx, {"_order": [], "last": None, "wins": 0})

        # New token: create counter and put at front of LRU-like order.
        if host not in s:
            s[host] = 0
            s["_order"].insert(0, host)

        # Count this appearance.
        s[host] += 1

        # Simple decay when cumulative counts exceed majority_window.
        total_counts = sum(v for k, v in s.items() if isinstance(s.get(k), int))
        if total_counts > majority_window:
            # Remove one count from the tail-most token in _order with a positive count.
            for key in list(s["_order"])[::-1]:
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        # Drop tokens that decayed to zero; keep _order in sync.
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

        # Sticky modal "last": encourage stability unless confidence truly shifts.
        modal = max(
           (
              k for k in s
              if isinstance(s.get(k), int) and k not in CONTROL_KEYS
           ),
           key=lambda k: s[k],
           default=None
        )
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


# =============================================================================
# Path-change hygiene (reset/realign policies)
# =============================================================================

def _first_diff_index(prev_hops: list, curr_hops: list) -> Optional[int]:
    """Return the first hop index (1-based) where host tokens differ; None if identical length/content."""
    n = min(len(prev_hops), len(curr_hops))
    for i in range(n):
        if (prev_hops[i] or {}).get("host") != (curr_hops[i] or {}).get("host"):
            return i + 1
    if len(prev_hops) != len(curr_hops):
        return n + 1
    return None


def _reset_stats_from(stats: dict, start_hop_int: int, logger=None) -> None:
    """Remove stats buckets for hop indices >= start_hop_int (inclusive)."""
    to_del = [k for k in stats.keys() if k.isdigit() and int(k) >= start_hop_int]
    for k in to_del:
        stats.pop(k, None)
    if logger:
        logger.debug(f"[labels] reset stats from hop {start_hop_int} (inclusive); removed {len(to_del)} entries)")


def _realign_then_reset(stats: dict, prev_hops: list, curr_hops: list, logger=None) -> None:
    """
    Attempt to carry over buckets by matching the previous modal token to the
    new hop indices, then ensure a full set of buckets for current hops.
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
    """Apply the configured reset policy when a path change is detected."""
    first_diff = _first_diff_index(prev_hops, curr_hops)
    if first_diff is None:
        return
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
    _reset_stats_from(stats, first_diff, logger=logger)  # default: from_first_diff


# =============================================================================
# Decide labels per hop and persist <ip>_hops.json (consumed by rrd_exporter/html)
# =============================================================================

def _decide_label_per_hop(
    stats: dict,
    hops_json_path: str,
    unstable_threshold: float,
    topk_to_show: int,
    logger=None
) -> dict:
    """
    Decide the visible legend label per hop based on rolling statistics:
      - If one token clearly dominates (share >= unstable_threshold, or no competition), show the stable token.
      - If multiple tokens compete and the top share is below threshold, show "varies (a, b, ...)".
    Bookkeeping keys ('wins','last','_order') are never allowed to appear in labels.
    The literal '???' is kept when it appears from MTR.
    """
    labels = {}
    out = []

    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        hop_int = int(hop_str)
        if hop_int < 1:
            continue

        # Consider only real token counters; drop bookkeeping keys.
        items = [
            (k, s[k])
            for k in s
            if isinstance(s.get(k), int)
            and k not in IGNORE_HOSTS
            and k not in ("wins", "last", "_order")
        ]
        total = sum(c for _, c in items)
        if total == 0:
            continue

        # Sort by count descending; take the top candidate.
        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total

        if logger:
            logger.debug(
                f"[hop {hop_int}] label-calc total={total} top={top_host} "
                f"share={share:.2f} items={items[:topk_to_show]}"
            )

        # Build label text
        if share < unstable_threshold and len(items) >= 2:
            # Unstable competition → 'varies (a, b, ...)'.
            # Keep tokens as-is (including '???'); never include bookkeeping keys.
            sample_hosts: List[str] = []
            for token, _cnt in items[:topk_to_show]:
                if token in ("wins", "last", "_order"):
                    continue
                if token not in sample_hosts:
                    sample_hosts.append(token)
            host_label = f"varies ({', '.join(sample_hosts)})" if sample_hosts else "varies"
        else:
            # Stable case → prefer sticky modal 'last' if valid; otherwise current top.
            last_host = s.get("last")
            if last_host in ("wins", "last", "_order", None, ""):
                host_label = top_host
            else:
                host_label = last_host

        labels[hop_int] = f"{hop_int}: {host_label}"
        out.append({"count": hop_int, "host": host_label})

    # Persist hop labels (human-friendly legend) for rrd_exporter/html to use.
    if out:
        with open(hops_json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)

    return labels


# =============================================================================
# Main entrypoint
# =============================================================================

def monitor_target(ip: str, settings: Optional[dict] = None, **kwargs) -> None:
    """
    Canonical entrypoint used by the watchdog/controller.

    Parameters
    ----------
    ip : str
        Destination IP/host to monitor.
    settings : dict (required)
        Settings dict loaded via modules.utils.load_settings(...).
        Must include '_meta.settings_path' for hot-reload.
    kwargs :
        source_ip (optional): passed through to run_mtr(..).
        logger    (optional): logging.Logger to use; created if not provided.

    Behavior
    --------
    - Runs forever (until process is terminated).
    - Hot-reloads settings when the YAML file mtime changes.
    - Respects all path resolutions and logging levels from settings.
    """
    if settings is None:
        raise RuntimeError("monitor_target requires a 'settings' dict")

    # Optional extras
    source_ip = kwargs.get("source_ip")
    logger = kwargs.get("logger") or setup_logger(ip, settings=settings)

    # Resolve paths and ensure RRD directory exists (writer updates it).
    paths = resolve_all_paths(settings)
    rrd_dir = paths.get("rrd") or "data"
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")

    # Hot-reload state
    SETTINGS_FILE = _settings_path_from_settings(settings)
    last_settings_mtime = _safe_mtime(SETTINGS_FILE)

    # Initial settings consumption
    label_knobs = _label_cfg(settings)
    interval       = int(settings.get("interval_seconds", 60))
    severity_rules = settings.get("log_severity_rules", [])
    debug_rrd_log  = bool(settings.get("rrd", {}).get("debug_values", False))

    # Ensure the multi-hop RRD exists (single file, multiple DS for all hops/metrics).
    init_rrd(rrd_path, settings, logger)

    prev_hops: List[Dict[str, Any]] = []
    prev_loss_state: Dict[int, float] = {}

    logger.info(f"[{ip}] Monitoring loop started — interval={interval}s")

    while True:
        # ---------- Hot reload ----------
        curr_mtime = _safe_mtime(SETTINGS_FILE)
        if SETTINGS_FILE and curr_mtime > 0 and curr_mtime != last_settings_mtime:
            try:
                settings = load_settings(SETTINGS_FILE)
                refresh_logger_levels(settings)  # sync live logger levels

                # Re-resolve paths (rrd may change)
                paths = resolve_all_paths(settings)
                rrd_dir = paths.get("rrd") or rrd_dir
                os.makedirs(rrd_dir, exist_ok=True)
                rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")

                # Refresh knobs
                label_knobs    = _label_cfg(settings)
                interval       = int(settings.get("interval_seconds", interval))
                severity_rules = settings.get("log_severity_rules", severity_rules)
                debug_rrd_log  = bool(settings.get("rrd", {}).get("debug_values", debug_rrd_log))

                last_settings_mtime = curr_mtime
                logger.info(f"[{ip}] Settings reloaded. interval={interval}s, rrd_dir={rrd_dir}")
            except Exception as e:
                logger.error(f"[{ip}] Failed to hot-reload settings: {e}")

        # ---------- One MTR snapshot ----------
        hops = run_mtr(ip, source_ip, logger, settings=settings)
        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — unreachable/command failed")
            time.sleep(interval)
            continue

        hop_path_changed = hops_changed(prev_hops, hops)

        # ---------- Loss tracking per hop (hop >= 1) ----------
        curr_loss_state: Dict[int, float] = {}
        for h in hops:
            try:
                hop_num = int(h.get("count", 0))
            except (TypeError, ValueError):
                continue
            if hop_num < 1:
                continue
            loss = h.get("Loss%", 0.0)
            if loss is None:
                continue
            try:
                lf = float(loss)
            except (TypeError, ValueError):
                lf = 0.0
            if lf > 0.0:
                curr_loss_state[hop_num] = round(lf, 2)

        loss_changed = (curr_loss_state != prev_loss_state)

        # ---------- Path-change logging with severity ----------
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
                # Example key: "root[3]" → hop index 3 (0-based in diff → show raw index string)
                hop_index = key.split("[")[-1].rstrip("]")
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, (level or "info").lower(), logger.info)
                msg = f"[{ip}] Hop {hop_index} changed from {value.get('old_value')} to {value.get('new_value')}"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # ---------- Loss-change logging with severity ----------
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

        # ---------- Update multi-hop RRD ----------
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log, logger=logger)

        # ---------- Labels: apply reset policy BEFORE adding this snapshot ----------
        stats_path, hops_json_path = _label_paths(ip, settings)
        stats = _load_stats(stats_path)

        reset_mode         = label_knobs["reset_mode"]
        unstable_threshold = label_knobs["unstable_threshold"]
        topk_to_show       = label_knobs["topk_to_show"]
        majority_window    = label_knobs["majority_window"]
        sticky_min_wins    = label_knobs["sticky_min_wins"]

        if hop_path_changed:
            _apply_reset_policy(stats, prev_hops, hops, reset_mode, logger=logger)

        # Update stats with the current snapshot (hop >= 1 only)
        # Update counters
        stats = _update_stats_with_snapshot(stats, hops,
                                            majority_window=majority_window,
                                            sticky_min_wins=sticky_min_wins,
                                            logger=logger)
        # Save a sanitized version (removes wins/last/_order)
        _save_stats(stats_path, stats)
        # *** Reload the clean file ***
        clean_stats = _load_stats(stats_path)
        # Build labels from the clean snapshot only
        _decide_label_per_hop(
           clean_stats,
           hops_json_path,
           unstable_threshold=unstable_threshold,
           topk_to_show=topk_to_show,
           logger=logger
        )


        # Synchronize labels + write traceroute artifacts via strict writer
        update_hop_labels_only(ip, hops, settings, logger)
        if hop_path_changed or loss_changed:
            save_trace_and_json(ip, hops, settings, logger)
            logger.info(f"[{ip}] Traceroute and hop map saved.")
        else:
            logger.debug(f"[{ip}] No change detected — {len(hops)} hops parsed. RRD updated.")

        prev_hops = hops
        prev_loss_state = curr_loss_state
        time.sleep(interval)
