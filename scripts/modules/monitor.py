#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/monitor.py
==================

Purpose
-------
Run the monitoring loop for a single target (IP/host):

1) Execute one MTR snapshot per interval.
2) Detect path changes and hop loss changes; log with severity rules.
3) Update the multi-hop RRD for this target.
4) Delegate ALL traceroute artifact writing (stats + labels + optional trace JSON)
   to `modules.graph_utils.update_labels_and_traces` (single-writer model).

Configuration (YAML)
--------------------
- All knobs come from mtr_script_settings.yaml; no code defaults here.
- Required keys:
    paths:
      rrd:         /opt/scripts/MTR_WEB/data/
      traceroute:  /opt/scripts/MTR_WEB/traces/
      html:        /opt/scripts/MTR_WEB/html/
      graphs:      /opt/scripts/MTR_WEB/html/graphs/
      logs:        /opt/scripts/MTR_WEB/logs/
      fping:       /usr/sbin/fping
    interval_seconds: 60
    labels:
      reset_mode: "from_first_diff"   # one of: none | from_first_diff | realign_then_reset | all
      unstable_threshold: 0.45
      topk_to_show: 3
      majority_window: 200
      sticky_min_wins: 3
    rrd:
      debug_values: false
    log_severity_rules: []            # optional; used by modules.severity

Key dependencies
----------------
- modules.mtr_runner.run_mtr(ip, source_ip, logger, settings=settings) -> List[hop dicts]
- modules.rrd_handler.init_rrd(rrd_path, settings, logger)
- modules.rrd_handler.update_rrd(rrd_path, hops, ip, settings, debug_rrd_log, logger=logger)
- modules.severity.hops_changed(prev_hops, hops) -> bool
- modules.severity.evaluate_severity_rules(rules, context) -> (tag:str|None, level:str|None)
- modules.graph_utils.update_labels_and_traces(ip, hops, settings, write_trace_json, prev_hops, logger)
- modules.utils.load_settings / resolve_all_paths / setup_logger / refresh_logger_levels

Notes
-----
- Labels shown to users reflect exactly the MTR "Host" tokens (IPs/DNS/"???").
- Bookkeeping keys like "wins", "last", "_order" are **never** written to disk.
- Hot-reload: if the YAML file changes on disk, we reload and apply new knobs live.

CLI (for direct testing)
------------------------
You normally run this via mtr_watchdog.py. For a quick manual test:

    PYTHONPATH=/opt/scripts/MTR_WEB/scripts \
      python3 -m modules.monitor 8.8.8.8 --settings /opt/scripts/MTR_WEB/mtr_script_settings.yaml
"""

from __future__ import annotations

import os
import time
import argparse
from typing import Optional, Dict, Any, List, Tuple

from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, update_rrd
from modules.severity import evaluate_severity_rules, hops_changed
from modules.graph_utils import update_labels_and_traces
from modules.utils import (
    load_settings,
    resolve_all_paths,
    setup_logger,
    refresh_logger_levels,
)

# ---------------------------------------------------------------------------
# Helpers for settings hot-reload
# ---------------------------------------------------------------------------

def _settings_path_from_settings(settings: dict) -> Optional[str]:
    """Find the live YAML path for hot-reload."""
    meta = settings.get("_meta") or {}
    sp = meta.get("settings_path")
    if isinstance(sp, str) and os.path.isfile(sp):
        return sp
    # fallback: repo root / mtr_script_settings.yaml
    modules_dir = os.path.abspath(os.path.dirname(__file__))            # .../scripts/modules
    scripts_dir = os.path.abspath(os.path.join(modules_dir, os.pardir)) # .../scripts
    repo_root   = os.path.abspath(os.path.join(scripts_dir, os.pardir)) # repo root
    fallback = os.path.join(repo_root, "mtr_script_settings.yaml")
    return fallback if os.path.isfile(fallback) else None


def _safe_mtime(path: Optional[str]) -> float:
    """Return file mtime or 0.0 on error."""
    if not path:
        return 0.0
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Logging utilities for changes
# ---------------------------------------------------------------------------

def _format_hosts(hops: List[Dict[str, Any]]) -> List[str]:
    """Extract host tokens in order for simple diff logging."""
    out: List[str] = []
    for h in hops or []:
        host = h.get("host")
        out.append("" if host is None else str(host))
    return out


def _log_path_changes(ip: str, logger, prev: List[Dict[str, Any]], curr: List[Dict[str, Any]],
                      severity_rules: Optional[List[Dict[str, Any]]]) -> None:
    """Log hop sequence changes with severity mapping."""
    prev_hosts = _format_hosts(prev)
    curr_hosts = _format_hosts(curr)
    # Identify indices that changed (simple, order-sensitive)
    max_len = max(len(prev_hosts), len(curr_hosts))
    changed_indices: List[int] = []
    for i in range(max_len):
        a = prev_hosts[i] if i < len(prev_hosts) else None
        b = curr_hosts[i] if i < len(curr_hosts) else None
        if a != b:
            changed_indices.append(i + 1)  # hops are 1-based

    context = {
        "hop_changed": True,
        "changed_indices": changed_indices,
        "prev_path": prev_hosts,
        "curr_path": curr_hosts,
    }
    tag, level = evaluate_severity_rules(severity_rules, context)
    msg_base = f"[{ip}] Path changed at hops {changed_indices}: {prev_hosts}  ->  {curr_hosts}"
    if tag:
        msg_base = f"[{tag}] {msg_base}"
    log_fn = getattr(logger, (level or "info").lower(), logger.info)
    log_fn(msg_base)


def _extract_loss_state(hops: List[Dict[str, Any]]) -> Dict[int, float]:
    """
    Build {hop_index: loss_percent} from a single MTR snapshot.
    Only include hops where Loss% > 0.
    """
    out: Dict[int, float] = {}
    for h in hops or []:
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
            out[hop_num] = round(lf, 2)
    return out


def _log_loss_changes(ip: str, logger, prev_losses: Dict[int, float], curr_losses: Dict[int, float],
                      severity_rules: Optional[List[Dict[str, Any]]], hop_path_changed: bool) -> None:
    """Log loss changes per hop with severity mapping."""
    for hop_num, loss in curr_losses.items():
        context = {
            "loss": loss,
            "prev_loss": prev_losses.get(hop_num, 0.0),
            "hop_changed": hop_path_changed,
        }
        tag, level = evaluate_severity_rules(severity_rules, context)
        default_fn = logger.warning if loss > 0 else logger.info
        log_fn = getattr(logger, (level or ("WARNING" if loss > 0 else "INFO")).lower(), default_fn)
        msg = f"[{ip}] Loss at hop {hop_num}: {loss}% (prev: {context['prev_loss']}%)"
        log_fn(f"[{tag}] {msg}" if tag else msg)


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

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
    - Loops forever at `settings['interval_seconds']`.
    - Hot-reloads settings when the YAML mtime changes.
    - Writes RRD values every iteration.
    - Delegates all traceroute/label artifact writes to graph_utils (single writer).
    """
    if settings is None:
        raise RuntimeError("monitor_target requires a 'settings' dict'")

    source_ip = kwargs.get("source_ip")
    logger = kwargs.get("logger") or setup_logger(ip, settings=settings)

    # Paths and RRD priming
    paths = resolve_all_paths(settings)
    rrd_dir = paths["rrd"]
    os.makedirs(rrd_dir, exist_ok=True)
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")

    # Hot-reload state
    SETTINGS_FILE = _settings_path_from_settings(settings)
    last_settings_mtime = _safe_mtime(SETTINGS_FILE)

    # Strict pulls
    interval       = int(settings["interval_seconds"])
    severity_rules = settings.get("log_severity_rules")
    debug_rrd_log  = bool(settings["rrd"]["debug_values"])

    # Ensure RRD exists
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
                refresh_logger_levels(settings)
                paths = resolve_all_paths(settings)
                rrd_dir = paths["rrd"]
                os.makedirs(rrd_dir, exist_ok=True)
                rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")

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

        # ---------- Change detection ----------
        hop_path_changed = hops_changed(prev_hops, hops)
        curr_loss_state  = _extract_loss_state(hops)
        loss_changed     = (curr_loss_state != prev_loss_state)

        if hop_path_changed:
            _log_path_changes(ip, logger, prev_hops, hops, severity_rules)
        if loss_changed:
            _log_loss_changes(ip, logger, prev_loss_state, curr_loss_state, severity_rules, hop_path_changed)

        # ---------- Update RRD ----------
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log, logger=logger)

        # ---------- SINGLE WRITER: labels + traces ----------
        write_trace = bool(hop_path_changed or loss_changed)
        try:
            update_labels_and_traces(
                ip=ip,
                hops=hops,
                settings=settings,
                write_trace_json=write_trace,
                prev_hops=prev_hops,
                logger=logger,
            )
        except Exception as e:
            logger.error(f"[{ip}] graph_utils update failed: {e}")

        # ---------- Bookkeeping ----------
        prev_hops = hops
        prev_loss_state = curr_loss_state

        time.sleep(interval)


# ---------------------------------------------------------------------------
# Minimal CLI wrapper (manual testing)
# ---------------------------------------------------------------------------

def _resolve_settings_path(argv: List[str]) -> str:
    """Allow `-s/--settings` or positional path (legacy)."""
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("-s", "--settings", dest="settings", default=None)
    args, _ = ap.parse_known_args(argv)
    if args.settings:
        return os.path.abspath(args.settings)
    # look for first non-flag token
    for tok in argv:
        if not tok.startswith("-") and tok.count(".yaml"):
            return os.path.abspath(tok)
    # default: repo root
    here = os.path.abspath(os.path.dirname(__file__))
    scripts = os.path.abspath(os.path.join(here, os.pardir))
    root = os.path.abspath(os.path.join(scripts, os.pardir))
    return os.path.join(root, "mtr_script_settings.yaml")


if __name__ == "__main__":
    # Simple CLI to run a single target for quick tests
    parser = argparse.ArgumentParser(description="Run one monitor loop (manual test)")
    parser.add_argument("ip", help="IP/host to monitor")
    parser.add_argument("-s", "--settings", dest="settings", help="Path to YAML settings", default=None)
    parser.add_argument("--source", dest="source_ip", help="Source IP for MTR", default=None)
    ns = parser.parse_args()

    settings_path = _resolve_settings_path(
        [a for a in ["--settings", ns.settings] if a]  # normalize inputs
    ) if ns.settings else _resolve_settings_path([])

    cfg = load_settings(settings_path)
    log = setup_logger(ns.ip, settings=cfg)
    monitor_target(ns.ip, settings=cfg, source_ip=ns.source_ip, logger=log)
