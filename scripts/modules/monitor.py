#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/monitor.py
==================

Single responsibility
---------------------
This module **orchestrates** the monitoring loop for one target:
- Runs MTR snapshots on a fixed cadence.
- Detects path/loss changes and logs them with rule-based severity.
- Updates the per-target multi-hop RRD.
- Delegates **all** traceroute artifact writing (stats + labels + trace JSON)
  to `modules.graph_utils.update_labels_and_traces` (single-writer model).

Design goals
------------
1) **YAML-first configuration**  
   All tunables are read from `mtr_script_settings.yaml` and hot-reloaded.
   No hardcoded defaults here. If a required key is missing, let it fail fast
   so the YAML is fixed (keeps behavior explicit and reproducible).

2) **Single writer for traces**  
   `graph_utils.update_labels_and_traces(...)` is the **only** function that
   writes `*_hops_stats.json`, `*_hops.json`, and (optionally) `*_trace.json`.
   This prevents duplicate writes and guarantees sanitized output on disk.

3) **Separation of concerns**  
   - This file: scheduling, MTR execution, RRD update, change detection, logging.
   - `graph_utils.py`: per-hop stats maintenance, label decisions, sanitized persistence.
   - `rrd_handler.py`: RRD lifecycle and value writes.

Operational flow (each loop)
----------------------------
1) Read (and hot-reload) settings.
2) Run one `mtr --json` snapshot for the target.
3) Compare path/loss vs previous snapshot for change logging.
4) Update RRD with the snapshot metrics.
5) Call `graph_utils.update_labels_and_traces(...)` to:
   - update per-hop stats,
   - write sanitized `*_hops_stats.json`,
   - write human-friendly `*_hops.json` labels,
   - optionally write a full `*_trace.json` if anything changed.

Required settings (mtr_script_settings.yaml)
--------------------------------------------
paths:
  rrd:         /opt/scripts/MTR_WEB/data/
  traceroute:  /opt/scripts/MTR_WEB/traces/
  html:        /opt/scripts/MTR_WEB/html/
  graphs:      /opt/scripts/MTR_WEB/html/graphs/
  logs:        /opt/scripts/MTR_WEB/logs/
  fping:       /usr/sbin/fping

interval_seconds: 60

labels:                         # consumed inside graph_utils.update_labels_and_traces
  reset_mode: "from_first_diff" # one of: none | from_first_diff | realign_then_reset | all
  unstable_threshold: 0.45
  topk_to_show: 3
  majority_window: 200
  sticky_min_wins: 3

log_severity_rules: []          # optional list; see modules.severity

Dependencies
------------
- modules.mtr_runner.run_mtr
- modules.rrd_handler.init_rrd / update_rrd
- modules.severity.evaluate_severity_rules / hops_changed
- modules.graph_utils.update_labels_and_traces
- modules.utils.load_settings / resolve_all_paths / setup_logger / refresh_logger_levels

Notes
-----
- Labels shown to users reflect exactly the MTR "Host" column tokens
  (IPs, DNS names, and literal "???"). Any placeholder like "*", "?" or empty
  is normalized downstream to "(waiting for reply)". All bookkeeping keys
  (wins/last/_order) are **never** persisted to disk.

"""

from __future__ import annotations

import os
import time
from typing import Optional, Dict, Any, List

from deepdiff import DeepDiff

from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, update_rrd
from modules.severity import evaluate_severity_rules, hops_changed
from modules.graph_utils import update_labels_and_traces
from modules.utils import load_settings, resolve_all_paths, setup_logger, refresh_logger_levels


# =============================================================================
# Settings helpers (strict YAML, hot-reload aware)
# =============================================================================

def _settings_path_from_settings(settings: dict) -> Optional[str]:
    """
    Locate the live settings file path for hot-reload:
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
    """Return file mtime or 0.0 on error (for hot-reload checks)."""
    if not path:
        return 0.0
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


# =============================================================================
# Main entrypoint
# =============================================================================

def monitor_target(ip: str, settings: Optional[dict] = None, **kwargs) -> None:
    """
    Run the monitoring loop for a single target.

    Parameters
    ----------
    ip : str
        Destination IP/host to monitor.
    settings : dict
        YAML-loaded settings. Must include '_meta.settings_path' for hot-reload.
    kwargs :
        source_ip (optional): source address for MTR (passed to run_mtr).
        logger    (optional): logging.Logger instance to use.

    Behavior
    --------
    - Loops indefinitely at `settings['interval_seconds']`.
    - Hot-reloads settings when the YAML mtime changes.
    - Writes RRD values every iteration.
    - Delegates all traceroute/label artifact writes to graph_utils (single writer).
    """
    if settings is None:
        raise RuntimeError("monitor_target requires a 'settings' dict")

    # Optional extras
    source_ip = kwargs.get("source_ip")
    logger = kwargs.get("logger") or setup_logger(ip, settings=settings)

    # Resolve paths and ensure RRD directory exists (writer updates it).
    paths = resolve_all_paths(settings)
    rrd_dir = paths["rrd"]  # strict: must exist in YAML (resolve_all_paths ensures)
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")

    # Hot-reload state
    SETTINGS_FILE = _settings_path_from_settings(settings)
    last_settings_mtime = _safe_mtime(SETTINGS_FILE)

    # Strict YAML pulls (no code defaults)
    interval       = int(settings["interval_seconds"])
    severity_rules = settings.get("log_severity_rules")  # optional
    debug_rrd_log  = bool(settings["rrd"]["debug_values"])

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

                # Re-resolve paths (rrd dir may change)
                paths = resolve_all_paths(settings)
                rrd_dir = paths["rrd"]
                os.makedirs(rrd_dir, exist_ok=True)
                rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")

                # Refresh strict pulls
                interval       = int(settings["interval_seconds"])
                severity_rules = settings.get("log_severity_rules")
                debug_rrd_log  = bool(settings["rrd"]["debug_values"])

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

        # ---------- Path change detection ----------
        hop_path_changed = hops_changed(prev_hops, hops)

        # ---------- Loss tracking (aggregate % per hop from snapshot) ----------
        curr_loss_state: Dict[int, float] = {}
        for h in hops:
            try:
                hop_num = int(h.get("count", 0))
            except (TypeError, ValueError):
                continue
            if hop_num < 1:
                continue
            try:
                lf = float(h.get("Loss%", 0.0) or 0.0)
            except (TypeError, ValueError):
                lf = 0.0
            if lf > 0.0:
                curr_loss_state[hop_num] = round(lf, 2)

        loss_changed = (curr_loss_state != prev_loss_state)

        # ---------- Change logging with severity ----------
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
                # Example key: "root[3]" → hop index 3 (0-based in diff)
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

        # ---------- Update RRD with current snapshot ----------
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log, logger=logger)

        # ---------- SINGLE WRITER: graph_utils handles all traceroute artifacts ----------
        write_trace = bool(hop_path_changed or loss_changed)
        try:
            update_labels_and_traces(
                ip=ip,
                hops=hops,
                settings=settings,
                write_trace_json=write_trace,
                prev_hops=prev_hops,
                logger=logger
            )
        except Exception as e:
            logger.error(f"[{ip}] graph_utils update failed: {e}")

        # ---------- Bookkeeping for next iteration ----------
        prev_hops = hops
        prev_loss_state = curr_loss_state

        time.sleep(interval)
