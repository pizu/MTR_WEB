#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/utils.py
================

Shared utilities for the MTR_WEB project.

What this module provides
-------------------------
- Settings I/O:
    * load_settings(path) -> dict

- Paths (strict + consistent):
    * resolve_all_paths(settings) -> dict with: html, graphs, logs, rrd, traceroute, cache
      - html/graphs/logs/cache are created if missing
      - rrd and traceroute are inputs; we DO NOT create them
      - traceroute is STRICTLY taken from YAML (no env / no legacy fallbacks)
    * resolve_html_dir(settings)  -> <paths.html> (ensures exists)
    * resolve_graphs_dir(settings)-> <paths.graphs> (ensures exists)
    * resolve_targets_path(settings=None) -> absolute path to mtr_targets.yaml
      (accepts both old call style w/o args and new style with settings)

- HTML / graph helpers:
    * get_html_ranges(settings) -> sanitized list of {"label","seconds"}
    * resolve_html_knobs(settings) -> (auto_refresh_seconds, log_lines_display)
    * resolve_canvas(settings) -> (width, height, max_hops)

- Logging:
    * setup_logger(name, settings, ...) -> logging.Logger (rotating file + console)
    * refresh_logger_levels(...) -> works with BOTH new and old signatures
        - New: refresh_logger_levels(settings, logger_names=[...])
        - Old: refresh_logger_levels(logger, "logger_name", settings)

Design notes
------------
- This file never assumes a global "repo root" except where needed by
  resolve_targets_path() as a final fallback. We compute repo_root()
  from this file's location so systemd or different cwd won't break it.
- Traceroute dir is **strict**: must be exactly settings['paths']['traceroute'] and
  must exist. Writers will refuse; readers log and continue with empty labels.

"""

from __future__ import annotations

import os
import sys
import json
import time
import yaml
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

# -----------------------------------------------------------------------------
# Small internals
# -----------------------------------------------------------------------------

def _expand(path: Optional[str]) -> Optional[str]:
    """Expand ~ and env vars; return absolute path or None."""
    if not path:
        return None
    return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))

def _mkdir_p(p: Optional[str]) -> None:
    if not p:
        return
    os.makedirs(p, exist_ok=True)

def _read_yaml(fp: str) -> Dict[str, Any]:
    with open(fp, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}

def repo_root() -> str:
    """
    Heuristic repo root = two levels above this file:
      .../scripts/modules/utils.py -> repo_root = .../
    """
    return str(Path(__file__).resolve().parents[2])

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

def load_settings(path: str) -> Dict[str, Any]:
    """
    Load YAML settings from 'path', expand known path keys, ensure key presence.

    Ensures:
      - top-level dicts: 'paths', 'logging_levels'
      - expands: paths.[rrd|graphs|html|logs|traceroute|cache]
    """
    path = _expand(path) or path
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"Settings file not found: {path}")
    data = _read_yaml(path)
    data.setdefault("paths", {})
    data.setdefault("logging_levels", {})
    p = data["paths"]
    for k in ("rrd", "graphs", "html", "logs", "traceroute", "cache"):
        if k in p:
            p[k] = _expand(p[k])
    return data

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

def get_path(settings: dict, key: str, legacy_keys: Optional[List[str]] = None, default: Optional[str] = None) -> Optional[str]:
    """
    Resolve a path-like value.

    - Looks in settings['paths'][key]
    - If absent and legacy_keys provided, tries those at top-level
    - Returns the string as-is (no existence check)
    """
    try:
        paths = settings.get("paths", {}) or {}
        val = paths.get(key)
        if val:
            return str(val)
    except Exception:
        pass

    if legacy_keys:
        for k in legacy_keys:
            try:
                v = settings.get(k)
                if v:
                    return str(v)
            except Exception:
                continue
    return default

def resolve_all_paths(settings: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Resolve all important directories. STRICT policy for traceroute:

    Returns a dict:
      {
        "html": <str>,         # created
        "graphs": <str>,       # created
        "logs": <str>,         # created
        "cache": <str>,        # created (default <html>/var/hop_ip_cache)
        "rrd": <str|None>,     # input; not created
        "traceroute": <str|None>  # input; STRICT; not created
      }
    """
    paths_cfg = (settings or {}).get("paths", {}) or {}

    html_dir   = paths_cfg.get("html")   or _expand("./html")
    logs_dir   = paths_cfg.get("logs")   or _expand("./logs")
    graphs_dir = paths_cfg.get("graphs") or os.path.join(html_dir, "graphs")
    cache_dir  = paths_cfg.get("cache")  or os.path.join(html_dir, "var", "hop_ip_cache")

    _mkdir_p(html_dir)
    _mkdir_p(logs_dir)
    _mkdir_p(graphs_dir)
    _mkdir_p(cache_dir)

    # Inputs: do not create
    rrd_dir = paths_cfg.get("rrd")
    if rrd_dir:
        rrd_dir = _expand(rrd_dir)

    # STRICT traceroute path: only from YAML and must exist
    tr_dir = paths_cfg.get("traceroute")
    traceroute_dir = _expand(tr_dir) if tr_dir else None
    if traceroute_dir and not os.path.isdir(traceroute_dir):
        traceroute_dir = None
        log = logging.getLogger("paths")
        if log.handlers:
            log.error("settings.paths.traceroute is missing or not a directory. "
                      "Writers must refuse to write; readers will have empty hop labels.")

    return {
        "html": html_dir,
        "graphs": graphs_dir,
        "logs": logs_dir,
        "cache": cache_dir,
        "rrd": rrd_dir,
        "traceroute": traceroute_dir,
    }

def resolve_html_dir(settings: Dict[str, Any]) -> str:
    p = resolve_all_paths(settings)["html"]
    _mkdir_p(p)
    return p

def resolve_graphs_dir(settings: Dict[str, Any]) -> str:
    p = resolve_all_paths(settings)["graphs"]
    _mkdir_p(p)
    return p

def resolve_targets_path(settings: Optional[Dict[str, Any]] = None) -> str:
    """
    Return an absolute path to the targets file (mtr_targets.yaml).

    Priority:
      1) settings['files']['targets'] if provided
      2) <repo_root>/mtr_targets.yaml
      3) ./mtr_targets.yaml (CWD)

    Accepts both call styles:
      resolve_targets_path()                # old call sites
      resolve_targets_path(settings_dict)   # new call sites
    """
    # Try settings override first
    if isinstance(settings, dict):
        files = settings.get("files") or {}
        cand = files.get("targets")
        if cand:
            return os.path.abspath(_expand(cand) or cand)

    # Repo-root default
    rr = repo_root()
    p1 = os.path.join(rr, "mtr_targets.yaml")
    if os.path.isfile(p1):
        return p1

    # Fallback: current working directory
    return os.path.abspath("./mtr_targets.yaml")

# -----------------------------------------------------------------------------
# HTML & Graph helpers
# -----------------------------------------------------------------------------

def get_html_ranges(settings: Dict[str, Any]) -> List[Dict[str, int]]:
    """
    Return a sanitized list of time ranges for the UI/exports.
    Accepts either:
      settings['graph_time_ranges'] = [{"label": "1h", "seconds": 3600}, ...]
      settings['graph_time_ranges'] = {"1h": 3600, "24h": 86400}
      settings['graph_time_ranges'] = ["1h:3600", "24h:86400"]
    """
    raw = settings.get("graph_time_ranges") or []
    out: List[Dict[str, int]] = []
    seen = set()

    if isinstance(raw, dict):
        for k, v in raw.items():
            label = str(k).strip()
            try:
                sec = int(v)
            except Exception:
                continue
            if not label or sec <= 0 or label in seen:
                continue
            seen.add(label)
            out.append({"label": label, "seconds": sec})

    elif isinstance(raw, list):
        for row in raw:
            label, sec = None, None
            if isinstance(row, dict):
                label = str(row.get("label") or "").strip()
                try:
                    sec = int(row.get("seconds"))
                except Exception:
                    sec = None
            elif isinstance(row, str):
                parts = row.split(":")
                if len(parts) == 2:
                    label = parts[0].strip()
                    try:
                        sec = int(parts[1].strip())
                    except Exception:
                        sec = None
            if not label or not sec or sec <= 0 or label in seen:
                continue
            seen.add(label)
            out.append({"label": label, "seconds": sec})

    out.sort(key=lambda r: r["seconds"])
    return out

def resolve_html_knobs(settings: Dict[str, Any]) -> tuple[int, int]:
    """
    Return HTML knobs:
      (auto_refresh_seconds, log_lines_display)

    Supports both:
      settings['html']['auto_refresh_seconds'] / settings['html']['log_lines_display']
    and legacy:
      settings['html_auto_refresh_seconds'], settings['log_lines_display']
    """
    html = settings.get("html", {}) or {}
    auto_refresh = html.get("auto_refresh_seconds",
                            settings.get("html_auto_refresh_seconds", 0))
    log_lines = html.get("log_lines_display",
                         settings.get("log_lines_display", 50))
    try: auto_refresh = int(auto_refresh)
    except Exception: auto_refresh = 0
    try: log_lines = int(log_lines)
    except Exception: log_lines = 50
    return auto_refresh, log_lines

def resolve_canvas(settings: Dict[str, Any]) -> tuple[int, int, int]:
    """
    Return graph canvas (width, height, max_hops) with legacy fallbacks.
    Matches the expectation in modules/graph_config.py:
        self.WIDTH, self.HEIGHT, self.MAX_HOPS = resolve_canvas(settings)
    """
    canvas = settings.get("graph_canvas", {}) or {}
    width    = canvas.get("width",  settings.get("graph_width",  800))
    height   = canvas.get("height", settings.get("graph_height", 200))
    max_hops = canvas.get("max_hops", settings.get("max_hops", 30))
    try: width = int(width)
    except Exception: width = 800
    try: height = int(height)
    except Exception: height = 200
    try: max_hops = int(max_hops)
    except Exception: max_hops = 30
    return width, height, max_hops

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
    - Level comes from settings['logging_levels'][name] (fallback 'default': INFO)
    - Rotating file handler + console handler
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    default_level = logging.INFO
    if settings:
        levels = settings.get("logging_levels", {}) or {}
        level_name = levels.get(name, levels.get("default", "INFO"))
        default_level = _level_from_name(level_name, logging.INFO)

    if level_override:
        default_level = _level_from_name(level_override, default_level)

    logger.setLevel(default_level)
    logger.propagate = False

    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    if settings:
        paths = resolve_all_paths(settings)
        logs_dir = paths["logs"] or _expand("./logs")
        _mkdir_p(logs_dir)
        if not logfile:
            logfile = os.path.join(logs_dir, f"{name}.log")
        fh = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh.setLevel(default_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(default_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.debug(f"Logger '{name}' initialized at level {logging.getLevelName(default_level)}")
    return logger

def refresh_logger_levels(*args, **kwargs) -> None:
    """
    Backward-compatible logger level refresher.

    Two accepted signatures:

    NEW (bulk refresh):
        refresh_logger_levels(settings, logger_names=None)
            - settings: dict
            - logger_names: optional list[str], default = all known loggers

    OLD (single logger refresh; used by older controller/monitor code):
        refresh_logger_levels(logger, name, settings)

    In both cases, handlers' levels are kept in sync with the logger level.
    """
    # Detect which call style is used
    if args and isinstance(args[0], dict):
        # NEW: (settings, logger_names=None)
        settings: Dict[str, Any] = args[0]
        logger_names: Optional[List[str]] = kwargs.get("logger_names")
        levels_cfg = settings.get("logging_levels", {}) or {}
        default_level = _level_from_name(levels_cfg.get("default", "INFO"))

        if logger_names is None:
            # all known non-proxy loggers
            names_to_consider = [
                n for (n, obj) in logging.root.manager.loggerDict.items()
                if isinstance(obj, logging.Logger)
            ]
            if "root" not in names_to_consider:
                names_to_consider.append("root")
        else:
            names_to_consider = list(logger_names)

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

    else:
        # OLD: (logger, name, settings)
        if len(args) < 3:
            return
        logger_obj: logging.Logger = args[0]
        name: str = args[1]
        settings: Dict[str, Any] = args[2] if isinstance(args[2], dict) else {}
        levels_cfg = settings.get("logging_levels", {}) or {}
        level = _level_from_name(levels_cfg.get(name, levels_cfg.get("default", "INFO")), logging.INFO)
        try:
            logger_obj.setLevel(level)
        except Exception:
            return
        for h in getattr(logger_obj, "handlers", []):
            try:
                h.setLevel(level)
            except Exception:
                pass
