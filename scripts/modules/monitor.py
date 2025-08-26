#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/monitor.py
==================

Monitors one target IP/host in a loop:

  1) Run an MTR snapshot (modules.mtr_runner.run_mtr).
  2) Detect hop-path or loss changes for logging/severity.
  3) Update the single multi-hop RRD file (<paths.rrd>/<ip>.rrd).
  4) Maintain rolling hop-label stats that drive "varies (...)" decisions.
  5) Persist traceroute artifacts (strictly under YAML paths.traceroute) via modules.graph_utils.
  6) Hot-reload mtr_script_settings.yaml without restart, and refresh logger levels.

Key design choices
------------------
- Strict traceroute path: read/write ONLY under settings['paths']['traceroute'].
  The writer is centralized in modules.graph_utils.* functions.
- Canonical entrypoint signature: monitor_target(ip, settings=None, **kwargs)
  Compatible with the watchdog calling convention. 'source_ip' and 'logger'
  can be passed through kwargs if desired.

Inputs
------
- ip (str):      Target IP/host.
- settings (dict): YAML settings loaded by modules.utils.load_settings(..).
                   The dict should include settings['_meta']['settings_path'].
- kwargs:
    - source_ip (optional):  Source address for MTR runner.
    - logger (optional):     logging.Logger to reuse; created if not supplied.

Outputs
-------
- Updates <paths.rrd>/<ip>.rrd each iteration.
- Writes/updates:
    <paths.traceroute>/<ip>_hops_stats.json
    <paths.traceroute>/<ip>_hops.json
    <paths.traceroute>/<ip>.trace.txt
    <paths.traceroute>/<ip>.json

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


# ---------------------------------------------------------------------------
# Label tunables (can be overridden by YAML at runtime)
# ---------------------------------------------------------------------------

UNSTABLE_THRESHOLD = 0.45   # if top share < threshold and competition exists => "varies (...)"
TOPK_TO_SHOW       = 3
MAJORITY_WINDOW    = 200
STICKY_MIN_WINS    = 3
IGNORE_HOSTS       = set()  # hosts to ignore in label tallies (optional)


# ---------------------------------------------------------------------------
# YAML-driven label config helper
# ---------------------------------------------------------------------------

def _label_cfg(settings: dict) -> dict:
    labels = settings.get("labels") or {}
    return {
        "reset_mode":         str(labels.get("reset_mode", "from_first_diff")).strip().lower(),
        "unstable_threshold": float(labels.get("unstable_threshold", UNSTABLE_THRESHOLD)),
        "topk_to_show":       int(labels.get("topk_to_show",       TOPK_TO_SHOW)),
        "majority_window":    int(labels.get("majority_window",    MAJORITY_WINDOW)),
        "sticky_min_wins":    int(labels.get("sticky_min_wins",    STICKY_MIN_WINS)),
    }


# ---------------------------------------------------------------------------
# Traceroute/label file paths (strictly under YAML paths.traceroute)
# ---------------------------------------------------------------------------

def _label_paths(ip: str, settings: dict) -> tuple[str, str]:
    """
    Return (stats_path, hops_json_path) under the STRICT traceroute directory.

    The traceroute directory is resolved via resolve_all_paths(settings)['traceroute'].
    The directory is created if missing (writer-only logic).
    """
    paths = resolve_all_paths(settings)
    tr_dir = paths.get("traceroute")
    if not tr_dir:
        raise RuntimeError("settings.paths.traceroute missing or not a directory (cannot write traceroute artifacts)")
    os.makedirs(tr_dir, exist_ok=True)
    stem = os.path.join(tr_dir, ip)
    return stem + "_hops_stats.json", stem + "_hops.json"


def _load_stats(stats_path: str) -> dict:
    try:
        with open(stats_path, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_stats(stats_path: str, stats: dict) -> None:
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)


# ---------------------------------------------------------------------------
# Stats maintenance (rolling counts, decay, sticky modal logic)
# ---------------------------------------------------------------------------

def _update_stats_with_snapshot(
    stats: dict,
    hops: list,
    majority_window: int,
    sticky_min_wins: int,
    logger=None
) -> dict:
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

        total_counts = sum(v for k, v in s.items() if isinstance(s.get(k), int))
        if total_counts > majority_window:
            # simple decay: remove one count from the tail-most item
            for key in list(s["_order"])[::-1]:
                if isinstance(s.get(key), int) and s[key] > 0:
                    s[key] -= 1
                    if s[key] == 0:
                        del s[key]
                        s["_order"] = [x for x in s["_order"] if x != key]
                    break

        # sticky modal
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


# ---------------------------------------------------------------------------
# Path-change hygiene (reset/realign policies)
# ---------------------------------------------------------------------------

def _first_diff_index(prev_hops: list, curr_hops: list) -> Optional[int]:
    n = min(len(prev_hops), len(curr_hops))
    for i in range(n):
        if (prev_hops[i] or {}).get("host") != (curr_hops[i] or {}).get("host"):
            return i + 1
    if len(prev_hops) != len(curr_hops):
        return n + 1
    return None


def _reset_stats_from(stats: dict, start_hop_int: int, logger=None) -> None:
    to_del = [k for k in stats.keys() if k.isdigit() and int(k) >= start_hop_int]
    for k in to_del:
        stats.pop(k, None)
    if logger:
        logger.debug(f"[labels] reset stats from hop {start_hop_int} (inclusive); removed {len(to_del)} entries)")


def _realign_then_reset(stats: dict, prev_hops: list, curr_hops: list, logger=None) -> None:
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


# ---------------------------------------------------------------------------
# Decide labels per hop and persist <ip>_hops.json
# ---------------------------------------------------------------------------

def _decide_label_per_hop(
    stats: dict,
    hops_json_path: str,
    unstable_threshold: float,
    topk_to_show: int,
    logger=None
) -> dict:
    labels = {}
    out = []

    for hop_str, s in sorted(stats.items(), key=lambda x: int(x[0])):
        hop_int = int(hop_str)
        if hop_int < 1:
            continue

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
        items.sort(key=lambda kv: -kv[1])
        top_host, top_count = items[0]
        share = top_count / total

        if logger:
            logger.debug(
                f"[hop {hop_int}] label-calc total={total} top={top_host} "
                f"share={share:.2f} items={items[:topk_to_show]}"
            )

        if share < unstable_threshold and len(items) >= 2:
          # Clean sample: skip bookkeeping keys, keep ??? if present
          sample_hosts = []
          for h, _ in items[:topk_to_show]:
            if h in ("wins", "last", "_order"):
              continue
              if h not in sample_hosts:
                sample_hosts.append(h)
                if not sample_hosts:
                  host_label = "varies"
                else:
                  host_label = f"varies ({', '.join(sample_hosts)})"
            else:
              # Fallback to stable last known or top host
              last_host = s.get("last")
              if last_host in ("wins", "last", "_order", None, ""):
                host_label = top_host
              else:
                host_label = last_host

        labels[hop_int] = f"{hop_int}: {host_label}"
        out.append({"count": hop_int, "host": host_label})

    if out:
        with open(hops_json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)

    return labels


# ---------------------------------------------------------------------------
# Settings path helpers (for hot-reload)
# ---------------------------------------------------------------------------

def _settings_path_from_settings(settings: dict) -> Optional[str]:
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
    if not path:
        return 0.0
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Main monitor entrypoint
# ---------------------------------------------------------------------------

def monitor_target(ip: str, settings: Optional[dict] = None, **kwargs) -> None:
    """
    Canonical entrypoint used by the watchdog/controller.

    Parameters
    ----------
    ip : str
        Destination IP/host to monitor.
    settings : dict | None
        Settings dict loaded via modules.utils.load_settings(...).
    kwargs :
        source_ip (optional): passed through to run_mtr(..).
        logger    (optional): logging.Logger to use; created if not provided.
    """
    if settings is None:
        raise RuntimeError("monitor_target requires a 'settings' dict")

    # Optional extras
    source_ip = kwargs.get("source_ip")
    logger = kwargs.get("logger") or setup_logger(ip, settings=settings)

    # Resolve paths and ensure RRD directory exists (writer updates it)
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

    # Ensure the multi-hop RRD exists
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
                label_knobs   = _label_cfg(settings)
                interval      = int(settings.get("interval_seconds", interval))
                severity_rules = settings.get("log_severity_rules", severity_rules)
                debug_rrd_log = bool(settings.get("rrd", {}).get("debug_values", debug_rrd_log))

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
        stats = _update_stats_with_snapshot(
            stats,
            hops,
            majority_window=majority_window,
            sticky_min_wins=sticky_min_wins,
            logger=logger
        )
        _save_stats(stats_path, stats)

        # Decide labels, write <ip>_hops.json (consumed by rrd_exporter/html)
        _decide_label_per_hop(
            stats,
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
