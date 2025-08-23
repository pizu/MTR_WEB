#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modules/utils.py
================

Utility helpers shared by all scripts:

- load_settings(path):            Load YAML settings into a dict.
- resolve_html_dir(settings):     Return the HTML output directory from YAML.
- resolve_all_paths(settings):    Resolve *all* important directories from YAML.
- setup_logger(name, ...):        Create/get a configured logger writing to the central logs dir.

This file enforces a **strict** policy for the traceroute directory:
  - Only settings['paths']['traceroute'] is accepted.
  - If it's missing or doesn't exist, we return None (and log an error).
  - No environment or legacy fallback is allowed for 'traceroute'.

Expected YAML (minimal):
------------------------
paths:
  rrd: /opt/scripts/MTR_WEB/data
  graphs: /opt/scripts/MTR_WEB/html/graphs
  html: /opt/scripts/MTR_WEB/html
  logs: /opt/scripts/MTR_WEB/logs
  traceroute: /opt/scripts/MTR_WEB/traceroute
  # Optional cache base (if omitted, we use <html>/var/hop_ip_cache):
  # cache: /opt/scripts/MTR_WEB/html/var/hop_ip_cache

logging_levels:
  default: INFO
  mtr_watchdog: INFO
  controller: INFO
  rrd_exporter: INFO
  html_generator: INFO
  graph_generator: INFO
  modules: WARNING

Notes
-----
- This module *creates* directories that are safe to materialize (html, graphs, logs, cache).
  It does NOT create 'rrd' or 'traceroute' automatically — those are considered data inputs.
- Rotating file logs are used to avoid unbounded growth.

"""

from __future__ import annotations

import os
import sys
import yaml
import json
import time
import errno
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional


# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------

def _expand(path: Optional[str]) -> Optional[str]:
    """Expand ~ and environment variables; return None for falsy inputs."""
    if not path:
        return None
    return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def _mkdir_p(path: str) -> None:
    """Create a directory (and parents) if it doesn't exist. No error if it does."""
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _read_yaml(fp: str) -> Dict[str, Any]:
    """Read a YAML file into a dict. Empty files produce {}."""
    with open(fp, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)  # type: ignore[no-redef]
    return data or {}


# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

def load_settings(path: str) -> Dict[str, Any]:
    """
    Load YAML settings from 'path' and normalize the structure a bit.
    Raises FileNotFoundError if the file doesn't exist.

    Returns
    -------
    dict : settings object
    """
    path = _expand(path) or path
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"Settings file not found: {path}")

    data = _read_yaml(path)

    # Ensure top-level dict keys exist
    data.setdefault("paths", {})
    data.setdefault("logging_levels", {})

    # Normalize paths: expand ~ and env vars
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
    Resolve all key directories from YAML. This function enforces the strict
    traceroute path policy (YAML only, no fallback).

    Returns
    -------
    dict with keys:
      - rrd         (str|None)  : RRD directory (no auto-create)
      - graphs      (str)       : HTML graphs directory (auto-create)
      - html        (str)       : HTML base directory (auto-create)
      - logs        (str)       : central logs directory (auto-create)
      - traceroute  (str|None)  : traceroute artifacts directory (YAML-only; no auto-create)
      - cache       (str)       : cache base (auto-create; defaults to <html>/var/hop_ip_cache)
    """
    # --- unpack paths (already expanded by load_settings)
    paths_cfg = settings.get("paths", {})
    rrd_dir   = paths_cfg.get("rrd")
    graphs    = paths_cfg.get("graphs")
    html_dir  = paths_cfg.get("html")
    logs_dir  = paths_cfg.get("logs")
    cache_dir = paths_cfg.get("cache")

    # html/logs/graphs: safe to create
    html_dir  = html_dir or _expand("./html")
    _mkdir_p(html_dir)

    logs_dir  = logs_dir or _expand("./logs")
    _mkdir_p(logs_dir)

    graphs    = graphs or os.path.join(html_dir, "graphs")
    _mkdir_p(graphs)

    # cache: default under HTML
    if not cache_dir:
        cache_dir = os.path.join(html_dir, "var", "hop_ip_cache")
    _mkdir_p(cache_dir)

    # rrd: data input; don't create if missing (that's a deployment issue)
    if rrd_dir:
        rrd_dir = _expand(rrd_dir)

    # ---- Traceroute (STRICT: YAML only; do not create)
    tr_yaml = paths_cfg.get("traceroute")
    traceroute_dir = _expand(tr_yaml) if tr_yaml else None
    if traceroute_dir and not os.path.isdir(traceroute_dir):
        # Do not create it. Treat as missing.
        traceroute_dir = None

    # Optional: light logging if global logging exists already
    log = logging.getLogger("paths")
    if log.handlers:  # avoid creating handlers here (setup_logger will do it)
        if traceroute_dir:
            log.info(f"Using traceroute dir (settings.paths.traceroute): {traceroute_dir}")
        else:
            log.error(
                "settings.paths.traceroute is missing or does not exist. "
                "Writers must refuse to write; readers will have empty hop labels."
            )

    return {
        "rrd": rrd_dir,
        "graphs": graphs,
        "html": html_dir,
        "logs": logs_dir,
        "traceroute": traceroute_dir,
        "cache": cache_dir,
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

    Parameters
    ----------
    name : str
        Logger name; also used as default filename <logs>/<name>.log
    settings : dict|None
        Settings dict with 'paths' and 'logging_levels'. If None, a console-only logger is created.
    logfile : str|None
        Explicit log file path. If None, use <paths.logs>/<name>.log
    level_override : str|None
        If provided, overrides the level computed from logging_levels.
    max_bytes : int
        RotatingFileHandler size threshold per file.
    backup_count : int
        Number of old log files to keep.

    Returns
    -------
    logging.Logger
    """
    logger = logging.getLogger(name)
    # If already configured, return as-is
    if logger.handlers:
        return logger

    # Decide level
    default_level = logging.INFO
    if settings:
        levels = settings.get("logging_levels", {})
        level_name = levels._
