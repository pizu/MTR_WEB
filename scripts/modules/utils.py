#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/utils.py
Shared utilities for the MTR_WEB project.

- Load/normalize YAML settings (injects _meta.settings_dir)
- Resolve core paths (strict on paths.traceroute)
- Targets path resolver (supports top-level 'targets:' file-path)
- Logging setup + dynamic level refresh
- HTML ranges helper (get_html_ranges)
- Small state writer (write_target_state)
"""

from __future__ import annotations

import os
import json
import yaml
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional


# -----------------------------------------------------------------------------
# Settings loading / normalization
# -----------------------------------------------------------------------------

def _abspath_relative_to(base_dir: str, maybe_path: Optional[str]) -> Optional[str]:
    if not maybe_path:
        return None
    p = str(maybe_path).strip()
    if not p:
        return None
    if os.path.isabs(p):
        return p
    return os.path.abspath(os.path.join(base_dir, p))


def load_settings(path: str) -> Dict[str, Any]:
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Settings file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    settings_dir = os.path.dirname(path)
    data.setdefault("_meta", {})
    data["_meta"]["settings_file"] = path
    data["_meta"]["settings_dir"] = settings_dir

    if isinstance(data.get("paths"), dict):
        for k, v in list(data["paths"].items()):
            if isinstance(v, str):
                data["paths"][k] = _abspath_relative_to(settings_dir, v)

    if isinstance(data.get("files"), dict):
        for k, v in list(data["files"].items()):
            if isinstance(v, str):
                data["files"][k] = _abspath_relative_to(settings_dir, v)

    # Allow top-level 'targets' as a file path string
    if isinstance(data.get("targets"), str):
        data["targets"] = _abspath_relative_to(settings_dir, data["targets"])

    return data


def resolve_all_paths(settings: Dict[str, Any]) -> Dict[str, str]:
    paths = settings.get("paths", {}) or {}
    out: Dict[str, str] = {}

    def must(key: str) -> Optional[str]:
        p = paths.get(key)
        if p:
            return os.path.abspath(p)
        return None

    # Existing keys
    out["data_dir"] = must("data_dir") or os.path.abspath("data")
    out["html_dir"] = must("html_dir") or os.path.abspath("html")
    out["html_data_dir"] = must("html_data_dir") or os.path.join(out["html_dir"], "data")
    out["logs_dir"] = must("logs_dir") or os.path.abspath("logs")

    tr = must("traceroute")
    if not tr:
        raise RuntimeError(
            "paths.traceroute is not configured in your YAML. "
            "Please set paths.traceroute explicitly."
        )
    out["traceroute"] = tr

    out["pipeline_logs_dir"] = must("pipeline_logs_dir") or os.path.join(out["logs_dir"], "")
    out["locks_dir"] = must("locks_dir") or os.path.join(out["data_dir"], ".locks")

    # NEW: Provide an explicit RRD directory for modules that expect 'rrd'
    # If paths.rrd is not set in YAML, alias it to data_dir to remain backward-compatible.
    rrd_dir = must("rrd") or out["data_dir"]
    out["rrd"] = rrd_dir

    return out


# -----------------------------------------------------------------------------
# Targets resolver
# -----------------------------------------------------------------------------

def resolve_targets_path(settings: Optional[Dict[str, Any]] = None) -> str:
    """
    Order:
      1) settings['files']['targets']
      2) settings['targets'] if it's a file path string
      3) mtr_targets.yaml next to the settings file
      4) mtr_targets.yaml in CWD
    """
    if isinstance(settings, dict):
        files = settings.get("files", {}) or {}
        p = files.get("targets")
        if p:
            if not os.path.isabs(p):
                base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
                p = os.path.join(base, p)
            return os.path.abspath(p)

        top = settings.get("targets")
        if isinstance(top, str) and top.strip():
            p = top.strip()
            if not os.path.isabs(p):
                base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
                p = os.path.join(base, p)
            return os.path.abspath(p)

        base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
        cand = os.path.join(base, "mtr_targets.yaml")
        if os.path.isfile(cand):
            return os.path.abspath(cand)

    return os.path.abspath("mtr_targets.yaml")


# -----------------------------------------------------------------------------
# Logging helpers
# -----------------------------------------------------------------------------

def _level_from_name(name: str, default=logging.INFO) -> int:
    try:
        return getattr(logging, str(name).upper())
    except Exception:
        return default


def setup_logger(
    name: str,
    settings: Optional[Dict[str, Any]] = None,
    *,
    level_override: Optional[str] = None,
    to_console: bool = True,
    to_file: bool = True,
    logfile_path: Optional[str] = None,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
    auto_refresh: bool = True,
) -> logging.Logger:
    logger = logging.getLogger(name)

    default_level = logging.INFO
    if settings:
        levels = settings.get("logging_levels", {}) or {}
        level_name = levels.get(name, levels.get("default", "INFO"))
        default_level = _level_from_name(level_name, logging.INFO)

    if level_override:
        default_level = _level_from_name(level_override, default_level)

    logger.setLevel(default_level)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    if to_console and not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(default_level)
        logger.addHandler(sh)

    if to_file:
        if not logfile_path:
            logs_dir = (settings.get("paths", {}) or {}).get("logs_dir") if settings else None
            logs_dir = logs_dir or os.path.abspath("logs")
            os.makedirs(logs_dir, exist_ok=True)
            logfile_path = os.path.join(logs_dir, f"{name}.log")

        if not any(isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", None) == logfile_path
                   for h in logger.handlers):
            os.makedirs(os.path.dirname(logfile_path), exist_ok=True)
            fh = RotatingFileHandler(logfile_path, maxBytes=max_bytes, backupCount=backup_count)
            fh.setFormatter(fmt)
            fh.setLevel(default_level)
            logger.addHandler(fh)

    if auto_refresh and settings:
        try:
            refresh_logger_levels(settings, keys=["default", name, "modules", "paths"])
        except Exception:
            pass

    return logger


def refresh_logger_levels(settings: Dict[str, Any], keys: Optional[List[str]] = None) -> None:
    levels = settings.get("logging_levels", {}) or {}

    def get_level(name: str) -> int:
        v = levels.get(name, levels.get("default", "INFO"))
        return _level_from_name(v, logging.INFO)

    for name in list(logging.Logger.manager.loggerDict.keys()):
        level = get_level(name)
        lg = logging.getLogger(name)
        try:
            lg.setLevel(level)
        except Exception:
            continue
        for h in lg.handlers:
            try:
                h.setLevel(level)
            except Exception:
                pass
        if lg.isEnabledFor(logging.DEBUG):
            lg.debug(f"Logger '{name}' level refreshed to {logging.getLevelName(level)}")


# -----------------------------------------------------------------------------
# HTML ranges helper (used by graph_config / graph_generator)
# -----------------------------------------------------------------------------

def get_html_ranges(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return the list of time ranges the HTML/graphs should render.

    Structure in YAML (under a single place, usually `graph.ranges`):
      graph:
        ranges:
          - { label: "15m", seconds: 900 }
          - { label: "1h",  seconds: 3600 }
          - { label: "6h",  seconds: 21600 }
          - { label: "12h", seconds: 43200 }
          - { label: "24h", seconds: 86400 }
          - { label: "1w",  seconds: 604800 }

    If absent, we fall back to a sensible default.
    """
    graph_cfg = settings.get("graph", {}) or {}
    ranges = graph_cfg.get("ranges")
    out: List[Dict[str, Any]] = []

    if isinstance(ranges, list) and ranges:
        for r in ranges:
            if not isinstance(r, dict):
                continue
            label = str(r.get("label") or "").strip()
            secs = r.get("seconds")
            try:
                secs = int(secs)
            except Exception:
                secs = None
            if label and secs and secs > 0:
                out.append({"label": label, "seconds": secs})

    if not out:
        out = [
            {"label": "15m", "seconds": 900},
            {"label": "1h",  "seconds": 3600},
            {"label": "6h",  "seconds": 21600},
            {"label": "12h", "seconds": 43200},
            {"label": "24h", "seconds": 86400},
            {"label": "1w",  "seconds": 604800},
        ]
    return out


# -----------------------------------------------------------------------------
# State file for UI (optional helper)
# -----------------------------------------------------------------------------

def write_target_state(state_dir: str, ip: str, status: str, reason: str = "") -> None:
    """
    Write a small json file the HTML can read to show current state.
    status: "running" | "paused" | "disabled" | "not_in_targets" | "error"
    """
    os.makedirs(state_dir, exist_ok=True)
    path = os.path.join(state_dir, f"{ip}_state.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"ip": ip, "status": status, "reason": reason, "ts": int(__import__("time").time())}, f)
