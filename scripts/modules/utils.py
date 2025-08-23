#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/utils.py
================

Shared utilities for the MTR_WEB project.

Key responsibilities
--------------------
- Settings loading (YAML) and normalization.
- Strict directory resolution:
    * html/graphs/logs/cache are safe to create.
    * rrd and traceroute are inputs; we do NOT auto-create them.
    * traceroute is STRICTLY taken from YAML (no env or legacy fallbacks).
- Logger setup with rotating file handler + console.
- Live refresh of logger levels from YAML (refresh_logger_levels).
- HTML/graph helpers:
    * get_html_ranges(settings) -> sanitized time range list for UI/exports.
    * resolve_canvas(settings)  -> convenience bundle the UI/readers can use.

YAML expectations (minimum)
---------------------------
paths:
  rrd: /opt/scripts/MTR_WEB/data
  graphs: /opt/scripts/MTR_WEB/html/graphs
  html: /opt/scripts/MTR_WEB/html
  logs: /opt/scripts/MTR_WEB/logs
  traceroute: /opt/scripts/MTR_WEB/traceroute
  # Optional:
  # cache: /opt/scripts/MTR_WEB/html/var/hop_ip_cache

graph_time_ranges:
  - label: "15m"
    seconds: 900
  - label: "1h"
    seconds: 3600
  - label: "24h"
    seconds: 86400

logging_levels:
  default: INFO
  controller: INFO
  mtr_watchdog: INFO
  rrd_exporter: INFO
  html_generator: INFO
  modules: WARNING
"""

from __future__ import annotations

import os
import sys
import yaml
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional, List


# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------

def _expand(path: Optional[str]) -> Optional[str]:
    """Expand ~ and environment variables; return absolute path or None."""
    if not path:
        return None
    return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def _mkdir_p(path: Optional[str]) -> None:
    """Create a directory (and parents) if it doesn't exist. No error if it does."""
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _read_yaml(fp: str) -> Dict[str, Any]:
    """Read a YAML file into a dict. Empty files produce {}."""
    with open(fp, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

def load_settings(path: str) -> Dict[str, Any]:
    """
    Load YAML settings from 'path' and normalize the structure.

    - Ensures top-level keys exist: 'paths', 'logging_levels'.
    - Expands ~ and environment variables for known path keys.

    Raises
    ------
    FileNotFoundError if the file is not found.
    """
    path = _expand(path) or path
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"Settings file not found: {path}")

    data = _read_yaml(path)
    data.setdefault("paths", {})
    data.setdefault("logging_levels", {})

    # Normalize/expand paths
    p = data["paths"]
    for k in ("rrd", "graphs", "html", "logs", "traceroute", "cache"):
        if k in p:
            p[k] = _expand(p[k])

    return data


def resolve_html_dir(settings: Dict[str, Any]) -> str:
    """
    Return the HTML output directory from settings; create it if missing.

    If paths.html is missing, default to "./html" (created).
    """
    html_dir = settings.get("paths", {}).get("html") or _expand("./html")
    _mkdir_p(html_dir)
    return html_dir


def resolve_all_paths(settings: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Resolve all important directories. STRICT policy for traceroute:
    - Only settings['paths']['traceroute'] is accepted.
    - If missing or not a directory, we return None (and log if a logger exists).

    Returns a dict:
      rrd         : str|None (input; not created)
      graphs      : str      (created if needed)
      html        : str      (created if needed)
      logs        : str      (created if needed)
      traceroute  : str|None (input; not created)
      cache       : str      (created if needed; default <html>/var/hop_ip_cache)
    """
    paths_cfg = settings.get("paths", {})

    # html/logs/graphs: safe to create
    html_dir = paths_cfg.get("html") or _expand("./html")
    _mkdir_p(html_dir)

    logs_dir = paths_cfg.get("logs") or _expand("./logs")
    _mkdir_p(logs_dir)

    graphs_dir = paths_cfg.get("graphs") or os.path.join(html_dir, "graphs")
    _mkdir_p(graphs_dir)

    # cache: default under HTML
    cache_dir = paths_cfg.get("cache") or os.path.join(html_dir, "var", "hop_ip_cache")
    _mkdir_p(cache_dir)

    # rrd: input; don't create
    rrd_dir = paths_cfg.get("rrd")
    if rrd_dir:
        rrd_dir = _expand(rrd_dir)

    # traceroute: STRICT YAML only; do not create
    tr_yaml = paths_cfg.get("traceroute")
    traceroute_dir = _expand(tr_yaml) if tr_yaml else None
    if traceroute_dir and not os.path.isdir(traceroute_dir):
        traceroute_dir = None

    log = logging.getLogger("paths")
    if log.handlers:
        if traceroute_dir:
            log.info(f"Using traceroute dir (settings.paths.traceroute): {traceroute_dir}")
        else:
            log.error(
                "settings.paths.traceroute is missing or not a directory. "
                "Writers must refuse to write; readers will have empty hop labels."
            )

    return {
        "rrd": rrd_dir,
        "graphs": graphs_dir,
        "html": html_dir,
        "logs": logs_dir,
        "traceroute": traceroute_dir,
        "cache": cache_dir,
    }


# -----------------------------------------------------------------------------
# HTML / Graph helpers
# -----------------------------------------------------------------------------

def get_html_ranges(settings: Dict[str, Any]) -> List[Dict[str, int]]:
    """
    Return a sanitized list of time ranges for the UI/exports.

    Input is expected under settings['graph_time_ranges'] as a list of dicts:
      - label (str)
      - seconds (int > 0)

    We:
      - coerce/validate types,
      - drop invalid/duplicate labels (first wins),
      - sort by 'seconds' ascending.

    Example output:
      [{"label": "15m", "seconds": 900}, {"label": "1h", "seconds": 3600}, ...]
    """
    raw = settings.get("graph_time_ranges") or []
    out: List[Dict[str, int]] = []
    seen = set()

    # Robust parsing
    if isinstance(raw, dict):
        # handle mapping forms like {"15m": 900, "1h": 3600}
        for k, v in raw.items():
            label = str(k).strip()
            try:
                seconds = int(v)
            except Exception:
                continue
            if not label or seconds <= 0 or label in seen:
                continue
            seen.add(label)
            out.append({"label": label, "seconds": seconds})

    elif isinstance(raw, list):
        for row in raw:
            label, seconds = None, None
            if isinstance(row, dict):
                label = str(row.get("label") or "").strip()
                try:
                    seconds = int(row.get("seconds"))
                except Exception:
                    seconds = None
            elif isinstance(row, str):
                # Accept "label:seconds" strings as a fallback (e.g., "1h:3600")
                parts = row.split(":")
                if len(parts) == 2:
                    label = parts[0].strip()
                    try:
                        seconds = int(parts[1].strip())
                    except Exception:
                        seconds = None
            if not label or not seconds or seconds <= 0 or label in seen:
                continue
            seen.add(label)
            out.append({"label": label, "seconds": seconds})

    # Sort by seconds (ascending)
    out.sort(key=lambda r: r["seconds"])
    return out


def resolve_canvas(settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience bundle for HTML/graph modules (e.g., graph_config.py).
    Returns:
      {
        "html_dir":   <paths.html>,
        "graph_dir":  <paths.graphs>,
        "time_ranges": get_html_ranges(settings),
      }
    """
    paths = resolve_all_paths(settings)
    return {
        "html_dir": paths["html"],
        "graph_dir": paths["graphs"],
        "time_ranges": get_html_ranges(settings),
    }


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR":    logging.ERROR,
    "WARNING":  logging.WARNING,
    "WARN":     logging.WARNING,
    "INFO":     logging.INFO,
    "DEBUG":    logging.DEBUG,
    "NOTSET":   logging.NOTSET,
}

def _level_from_name(name: Optional[str], default: int = logging.INFO) -> int:
    """Map a string level name (case-insensitive) to a logging level integer."""
    if not name:
        return default
    return _LEVELS.get(str(name).upper(), default)


def setup_logger(
    name: str,
    settings: Optional[Dict[str, Any]] = None,
    logfile: Optional[str] = None,
    level_override: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MiB per file
    backup_count: int = 5,
) -> logging.Logger:
    """
    Create (or retrieve) a logger that writes to the central logs directory.
    Adds both a rotating file handler (if settings are available) and a console handler.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    # Decide level from YAML logging_levels (or default INFO)
    default_level = logging.INFO
    if settings:
        levels = settings.get("logging_levels", {}) or {}
        level_name = levels.get(name, levels.get("default", "INFO"))
        default_level = _level_from_name(level_name, logging.INFO)

    if level_override:
        default_level = _level_from_name(level_override, default_level)

    logger.setLevel(default_level)
    logger.propagate = False  # don't duplicate to root

    # Formatter (timestamps + level + message)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # File handler (to central logs dir) if settings present
    if settings:
        all_paths = resolve_all_paths(settings)
        logs_dir = all_paths["logs"] or _expand("./logs")
        _mkdir_p(logs_dir)

        if not logfile:
            logfile = os.path.join(logs_dir, f"{name}.log")

        fh = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh.setLevel(default_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # Console handler (always add one)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(default_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.debug(f"Logger '{name}' initialized at level {logging.getLevelName(default_level)}")
    return logger


def refresh_logger_levels(settings: Dict[str, Any], logger_names: Optional[List[str]] = None) -> None:
    """
    Update logging levels for existing loggers according to settings['logging_levels'].

    Behavior:
      - Exact-name match first (e.g., 'controller', 'mtr_watchdog').
      - If name starts with 'modules' and 'modules' key exists, use that as a group default.
      - Otherwise fall back to 'default' (INFO if missing).
      - Each logger's handlers also get their level updated for consistency.
    """
    levels_cfg = (settings or {}).get("logging_levels", {})
    default_level = _level_from_name(levels_cfg.get("default", "INFO"))

    # Which loggers to touch?
    if logger_names:
        names_to_consider = logger_names
    else:
        names_to_consider = [
            n for (n, obj) in logging.root.manager.loggerDict.items()
            if isinstance(obj, logging.Logger)
        ]
        if "root" not in names_to_consider:
            names_to_consider.append("root")

    for name in names_to_consider:
        lg = logging.getLogger(None if name == "root" else name)

        level_name = levels_cfg.get(name)
        if not level_name and name.startswith("modules"):
            level_name = levels_cfg.get("modules")
        level = _level_from_name(level_name, default_level)

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
