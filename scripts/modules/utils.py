#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/utils.py
================

Shared utilities for the MTR_WEB project.

Key responsibilities
--------------------
- Load and normalize YAML settings.
- Resolve all key directories with a STRICT policy for the traceroute path
  (YAML only; no environment or legacy fallbacks; do not auto-create).
- Provide logging helpers (rotating file + console), with a live-level refresher.
- Provide HTML/graph helpers for time ranges and a convenience "canvas" bundle.
- Provide targets file resolution for exporters/controllers.

Minimum expected YAML
---------------------
paths:
  rrd: /opt/scripts/MTR_WEB/data
  graphs: /opt/scripts/MTR_WEB/html/graphs
  html: /opt/scripts/MTR_WEB/html
  logs: /opt/scripts/MTR_WEB/logs
  traceroute: /opt/scripts/MTR_WEB/traceroute
  # cache (optional): /opt/scripts/MTR_WEB/html/var/hop_ip_cache

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
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional, List


# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------

def _expand(path: Optional[str]) -> Optional[str]:
    """Expand ~ and env vars; return absolute path or None."""
    if not path:
        return None
    return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def _mkdir_p(path: Optional[str]) -> None:
    """Create a directory (and parents) if missing. No error if it exists."""
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
    Load YAML settings from 'path', expand known path keys, and attach metadata.

    Attaches:
      settings['_meta']['settings_path'] : absolute path to the YAML file
      settings['_meta']['settings_dir']  : parent directory of the YAML file
    """
    path = _expand(path) or path
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"Settings file not found: {path}")

    data = _read_yaml(path)
    data.setdefault("paths", {})
    data.setdefault("logging_levels", {})

    # Attach metadata for later relative path resolution
    data.setdefault("_meta", {})
    data["_meta"]["settings_path"] = path
    data["_meta"]["settings_dir"] = os.path.dirname(path)

    # Normalize/expand path fields
    p = data["paths"]
    for k in ("rrd", "graphs", "html", "logs", "traceroute", "cache"):
        if k in p:
            p[k] = _expand(p[k])

    return data


def resolve_html_dir(settings: Dict[str, Any]) -> str:
    """
    Return the HTML output directory; create it if missing.
    If paths.html is missing, default to ./html (created).
    """
    html_dir = settings.get("paths", {}).get("html") or _expand("./html")
    _mkdir_p(html_dir)
    return html_dir


def resolve_all_paths(settings: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Resolve all important directories.

    STRICT traceroute policy:
      - Accept ONLY settings['paths']['traceroute'].
      - Do NOT auto-create it.
      - If missing or not a directory, return None (consumers must handle this).

    Returns:
      {
        "rrd":        str|None,   # input; not created
        "graphs":     str,        # created if needed
        "html":       str,        # created if needed
        "logs":       str,        # created if needed
        "traceroute": str|None,   # input; not created
        "cache":      str,        # created if needed (defaults under html)
      }
    """
    paths_cfg = settings.get("paths", {}) or {}

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

    # rrd: input; do not create
    rrd_dir = paths_cfg.get("rrd")
    if rrd_dir:
        rrd_dir = _expand(rrd_dir)

    # traceroute: STRICT YAML only; do NOT create
    tr_yaml = paths_cfg.get("traceroute")
    traceroute_dir = _expand(tr_yaml) if tr_yaml else None
    if traceroute_dir and not os.path.isdir(traceroute_dir):
        traceroute_dir = None

    # Optional lightweight logging if a handler exists
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


def get_path(
    settings: Dict[str, Any],
    key: str,
    create: bool = False,
    default: Optional[str] = None,
    required: bool = False,
    **_ignore_kwargs,
) -> Optional[str]:
    """
    Resolve a single path by name, optionally creating safe directories.

    Parameters
    ----------
    settings : dict
        YAML settings loaded via load_settings().
    key : str
        One of: 'html', 'graphs', 'logs', 'cache', 'rrd', 'traceroute'.
    create : bool, default False
        If True, create the directory when it is safe to do so
        (only for: html, graphs, logs, cache). For 'rrd' and 'traceroute'
        this function NEVER creates the directory.
    default : str | None
        Fallback absolute/relative path to use if the resolved value is None.
        (Will be expanded to an absolute path.)
    required : bool, default False
        If True and the final path is still None/missing, raise RuntimeError.
    **_ignore_kwargs :
        Accepted and ignored to remain backward compatible with older callers
        that might pass extra flags like strict=True.

    Returns
    -------
    str | None
        The resolved absolute path, or None if not available and not required.

    Notes
    -----
    - Uses resolve_all_paths(settings), which enforces **strict YAML-only**
      resolution for 'traceroute' (no env/fallbacks; directory is not created).
    - For 'rrd' and 'traceroute', this helper never creates the directory.
    """
    paths = resolve_all_paths(settings)
    path = paths.get(key)

    if not path and default:
        path = _expand(default)

    # Only create for known "safe" outputs
    if path and create and key in ("html", "graphs", "logs", "cache"):
        _mkdir_p(path)

    if required and not path:
        raise RuntimeError(f"Required path '{key}' could not be resolved (and no usable default).")

    return path


# -----------------------------------------------------------------------------
# Targets file helper
# -----------------------------------------------------------------------------

def resolve_targets_path(settings: Optional[Dict[str, Any]] = None) -> str:
    """
    Return the absolute path to the targets YAML file.

    Order of resolution:
      1) settings['files']['targets'] (relative to the settings file if not absolute)
      2) 'mtr_targets.yaml' next to the settings file (if settings given)
      3) 'mtr_targets.yaml' in the current working directory

    Accepts both call styles:
      resolve_targets_path()                # ok
      resolve_targets_path(settings_dict)   # ok
    """
    if isinstance(settings, dict):
        files = settings.get("files", {}) or {}
        p = files.get("targets")
        if p:
            if not os.path.isabs(p):
                base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
                p = os.path.join(base, p)
            return os.path.abspath(p)

        base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
        cand = os.path.join(base, "mtr_targets.yaml")
        if os.path.isfile(cand):
            return os.path.abspath(cand)

    # Fallback if settings is None or above didn’t resolve
    return os.path.abspath("mtr_targets.yaml")


# -----------------------------------------------------------------------------
# HTML / Graph helpers
# -----------------------------------------------------------------------------

def get_html_ranges(settings: Dict[str, Any]) -> List[Dict[str, int]]:
    """
    Return a sanitized, sorted list of time ranges for UI/exports.

    Supports both the new and old config shapes:
      NEW (preferred):
        settings['html']['time_ranges'] = [
          {"label":"1h","seconds":3600}, {"label":"24h","seconds":86400}, ...
        ]
      OLD:
        settings['graph_time_ranges'] = same as above
        settings['graph_time_ranges'] = {"1h": 3600, "24h": 86400}
        settings['graph_time_ranges'] = ["1h:3600", "24h:86400"]

    De-duplicates by label (first wins) and sorts ascending by seconds.
    """
    # Prefer the new location first
    html_cfg = settings.get("html", {}) or {}
    raw = html_cfg.get("time_ranges")

    # Back-compat fallbacks
    if not raw:
        raw = settings.get("graph_time_ranges") or settings.get("time_ranges") or []

    out: List[Dict[str, int]] = []
    seen = set()

    # Mapping form: {"1h": 3600, "24h": 86400}
    if isinstance(raw, dict):
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

    # List form(s)
    elif isinstance(raw, list):
        for row in raw:
            label, seconds = None, None

            # Dict items: {"label": "...", "seconds": N}
            if isinstance(row, dict):
                label = str(row.get("label") or "").strip()
                try:
                    seconds = int(row.get("seconds"))
                except Exception:
                    seconds = None

            # String items: "1h:3600"
            elif isinstance(row, str) and ":" in row:
                a, b = row.split(":", 1)
                label = a.strip()
                try:
                    seconds = int(b.strip())
                except Exception:
                    seconds = None

            # Ignore anything else
            if not label or not seconds or seconds <= 0 or label in seen:
                continue
            seen.add(label)
            out.append({"label": label, "seconds": seconds})

    # Sort by seconds
    out.sort(key=lambda r: r["seconds"])
    return out

def resolve_html_knobs(settings: Dict[str, Any]) -> tuple[int, int]:
    """
    Return HTML tuning knobs as a tuple:
      (auto_refresh_seconds, log_lines_display)

    Picks from:
      - settings['html']['auto_refresh_seconds'] and ['html']['log_lines_display']
      - legacy fallbacks: settings['html_auto_refresh_seconds'], settings['log_lines_display']
    """
    html = settings.get("html", {}) or {}
    auto_refresh = html.get("auto_refresh_seconds",
                            settings.get("html_auto_refresh_seconds", 0))
    log_lines = html.get("log_lines_display",
                         settings.get("log_lines_display", 50))
    try:
        auto_refresh = int(auto_refresh)
    except Exception:
        auto_refresh = 0
    try:
        log_lines = int(log_lines)
    except Exception:
        log_lines = 50
    return auto_refresh, log_lines


def resolve_canvas(settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience bundle for HTML/graph modules.
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

    # Compute base level from YAML logging_levels (or default INFO)
    default_level = logging.INFO
    if settings:
        levels = settings.get("logging_levels", {}) or {}
        level_name = levels.get(name, levels.get("default", "INFO"))
        default_level = _level_from_name(level_name, logging.INFO)

    if level_override:
        default_level = _level_from_name(level_override, default_level)

    # Always enforce the level, even if handlers already exist.
    logger.setLevel(default_level)
    logger.propagate = False  # do not duplicate to root

    # If handlers already exist (e.g., previous init in-process), just retune them and return.
    if logger.handlers:
        for h in logger.handlers:
            try:
                h.setLevel(default_level)
            except Exception:
                pass
        return logger

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
