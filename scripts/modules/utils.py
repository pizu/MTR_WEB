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
- Lightweight helpers for HTML ranges and general config ergonomics.

Notes
-----
This file intentionally avoids any heavy dependencies beyond the Python stdlib
and PyYAML. It is safe to import from any script in the project.

Conventions
-----------
- All "paths" are absolute (resolved relative to settings file if user provides
  relative paths).
- `settings['_meta']['settings_dir']` is injected by load_settings() so other
  helpers can resolve relative paths consistently.

"""

from __future__ import annotations

import os
import sys
import json
import time
import yaml
import errno
import atexit
import socket
import shutil
import signal
import logging
import pathlib
import textwrap
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------------------------------------------------------
# Settings loading / normalization
# -----------------------------------------------------------------------------

def _abspath_relative_to(base_dir: str, maybe_path: Optional[str]) -> Optional[str]:
    """Return absolute path given a base directory."""
    if not maybe_path:
        return None
    p = str(maybe_path).strip()
    if not p:
        return None
    if os.path.isabs(p):
        return p
    return os.path.abspath(os.path.join(base_dir, p))


def load_settings(path: str) -> Dict[str, Any]:
    """
    Load YAML settings and inject a `_meta` section with:
      - settings_file (abs path)
      - settings_dir  (dir of the file)

    Also normalizes common path fields to absolute paths.
    """
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Settings file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # Inject meta for downstream helpers
    settings_dir = os.path.dirname(path)
    data.setdefault("_meta", {})
    data["_meta"]["settings_file"] = path
    data["_meta"]["settings_dir"] = settings_dir

    # Normalize path groups if present
    # - paths: { data_dir, html_dir, html_data_dir, logs_dir, traceroute, ... }
    if isinstance(data.get("paths"), dict):
        for k, v in list(data["paths"].items()):
            if isinstance(v, str):
                data["paths"][k] = _abspath_relative_to(settings_dir, v)

    # Legacy: files: { targets: ... }
    if isinstance(data.get("files"), dict):
        for k, v in list(data["files"].items()):
            if isinstance(v, str):
                data["files"][k] = _abspath_relative_to(settings_dir, v)

    # Allow top-level single paths (rare but used in some setups)
    if isinstance(data.get("targets"), str):
        data["targets"] = _abspath_relative_to(settings_dir, data["targets"])

    return data


def resolve_all_paths(settings: Dict[str, Any]) -> Dict[str, str]:
    """
    Return a dictionary of important absolute paths derived from settings.

    Expected keys:
      - data_dir, html_dir, html_data_dir, logs_dir, traceroute, etc.

    This function DOES NOT create directories automatically. Writers should
    create as needed.
    """
    paths = settings.get("paths", {}) or {}
    out = {}

    def must(key: str) -> Optional[str]:
        p = paths.get(key)
        if p:
            return os.path.abspath(p)
        return None

    out["data_dir"] = must("data_dir") or os.path.abspath("data")
    out["html_dir"] = must("html_dir") or os.path.abspath("html")
    out["html_data_dir"] = must("html_data_dir") or os.path.join(out["html_dir"], "data")
    out["logs_dir"] = must("logs_dir") or os.path.abspath("logs")

    # STRICT policy for traceroute path: *must* be set in YAML.
    tr = must("traceroute")
    if not tr:
        raise RuntimeError(
            "paths.traceroute is not configured in your YAML. "
            "Refusing to guess. Please set paths.traceroute explicitly."
        )
    out["traceroute"] = tr

    # Optional: pipeline logs directory (controller, per-script logs, etc.)
    out["pipeline_logs_dir"] = must("pipeline_logs_dir") or os.path.join(out["logs_dir"], "")

    # Optional: lock dir for single-writer patterns
    out["locks_dir"] = must("locks_dir") or os.path.join(out["data_dir"], ".locks")

    return out


# -----------------------------------------------------------------------------
# Targets resolver
# -----------------------------------------------------------------------------

def resolve_targets_path(settings: Optional[Dict[str, Any]] = None) -> str:
    """
    Return the absolute path to the targets YAML file.

    Order of resolution:
      1) settings['files']['targets'] (relative to the settings file if not absolute)
      2) settings['targets'] if it is a **string file path** (relative to the settings file if not absolute)
      3) 'mtr_targets.yaml' next to the settings file (if settings given)
      4) 'mtr_targets.yaml' in the current working directory

    Accepts both call styles:
      resolve_targets_path()                # ok
      resolve_targets_path(settings_dict)   # ok
    """
    if isinstance(settings, dict):
        # Legacy/structured location
        files = settings.get("files", {}) or {}
        p = files.get("targets")
        if p:
            if not os.path.isabs(p):
                base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
                p = os.path.join(base, p)
            return os.path.abspath(p)

        # NEW: allow top-level 'targets' to be a file path string
        top = settings.get("targets")
        if isinstance(top, str) and top.strip():
            p = top.strip()
            if not os.path.isabs(p):
                base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
                p = os.path.join(base, p)
            return os.path.abspath(p)

        # Conventional default next to settings
        base = settings.get("_meta", {}).get("settings_dir") or os.getcwd()
        cand = os.path.join(base, "mtr_targets.yaml")
        if os.path.isfile(cand):
            return os.path.abspath(cand)

    # Fallback if settings is None or above didn’t resolve
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
    """
    Create or reuse a configured logger.

    Parameters
    ----------
    name : str
        Logger name; also used to lookup per-logger level in YAML under
        `logging_levels.<name>` (falls back to `logging_levels.default`).
    settings : dict
        Settings dict loaded via load_settings(), or None.
    level_override : str
        Force a level (e.g., "DEBUG") ignoring YAML.
    to_console : bool
        Attach a stream handler to stderr.
    to_file : bool
        Attach a RotatingFileHandler under `paths.logs_dir`.
    logfile_path : str
        Full path to a logfile; overrides the default derived from logs_dir/name.
    max_bytes : int
        RotatingFileHandler maxBytes.
    backup_count : int
        RotatingFileHandler backupCount.
    auto_refresh : bool
        If True, call refresh_logger_levels(settings, [name,'modules','paths']) after setup.
    """
    logger = logging.getLogger(name)

    # Base level from YAML (or override)
    default_level = logging.INFO
    if settings:
        levels = settings.get("logging_levels", {}) or {}
        level_name = levels.get(name, levels.get("default", "INFO"))
        default_level = _level_from_name(level_name, logging.INFO)

    if level_override:
        default_level = _level_from_name(level_override, default_level)

    logger.setLevel(default_level)

    # Idempotent handler attachment
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    if to_console and not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(default_level)
        logger.addHandler(sh)

    # File logging
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
    """
    Refresh levels for all registered loggers according to `logging_levels`.

    Useful when your controller dynamically re-reads YAML and wants to push
    level changes to already-instantiated loggers.
    """
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
