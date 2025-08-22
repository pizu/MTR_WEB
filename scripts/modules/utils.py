#!/usr/bin/env python3
"""
utils.py - Shared utility functions for the MTR monitoring system.

Enhancements:
- Centralized handling of all filesystem paths via settings['paths'] (with legacy fallback).
- Unified logging configuration: levels under logging.levels, files under logging.files.
- HTML knobs and graph canvas resolved with helpers (backward compatible).
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union

# -------------------------------
# Settings helpers
# -------------------------------
def load_settings(settings_path: Optional[str] = None) -> Dict[str, Any]:
    """Load YAML settings (defaults to repo_root/mtr_script_settings.yaml)."""
    if settings_path is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        settings_path = os.path.join(base_dir, "mtr_script_settings.yaml")
    with open(settings_path, 'r') as f:
        return yaml.safe_load(f) or {}

def repo_root() -> str:
    """Absolute path to the repo root (../.. relative to this file)."""
    return str(Path(__file__).resolve().parents[2])

# -------------------------------
# Path resolution
# -------------------------------
def _get(settings: dict, dotted: str, default=None):
    """Fetch nested dict keys using dot notation."""
    cur = settings
    for part in dotted.split('.'):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def get_path(settings: dict, key: str, legacy_keys=None, default=None) -> str:
    """Resolve a path from settings['paths'][key] with fallback to legacy keys."""
    val = _get(settings, f"paths.{key}")
    if val:
        return str(val)
    if legacy_keys:
        for k in legacy_keys:
            if k in settings and settings[k]:
                return str(settings[k])
    return default

def resolve_all_paths(settings: dict) -> Dict[str, str]:
    """Return canonical paths with legacy fallbacks."""
    return {
        "logs":       get_path(settings, "logs",       ["log_directory"],           "logs"),
        "html":       get_path(settings, "html",       ["html_directory"],          "html"),
        "graphs":     get_path(settings, "graphs",     ["graph_output_directory"],  "html/graphs"),
        "rrd":        get_path(settings, "rrd",        ["rrd_directory"],           "data"),
        "traceroute": get_path(settings, "traceroute", ["traceroute_directory"],    "traceroute"),
        "fping":      get_path(settings, "fping",      ["fping_path"],              "/usr/sbin/fping"),
    }

def resolve_targets_path() -> str:
    """Absolute path to mtr_targets.yaml at repo root."""
    return str(Path(repo_root()) / "mtr_targets.yaml")

# -------------------------------
# HTML & graph helpers
# -------------------------------
def get_html_ranges(settings: dict):
    """Return html.time_ranges (new) or graph_time_ranges (legacy)."""
    return _get(settings, "html.time_ranges") or settings.get("graph_time_ranges", [])

def resolve_html_knobs(settings: dict):
    """Return HTML knobs: auto_refresh_seconds, log_lines_display."""
    html = settings.get("html", {})
    auto_refresh = html.get("auto_refresh_seconds", settings.get("html_auto_refresh_seconds", 0))
    log_lines_display = html.get("log_lines_display", settings.get("log_lines_display", 50))
    return auto_refresh, log_lines_display

def resolve_canvas(settings: dict):
    """Return graph canvas (width, height, max_hops) with legacy fallback."""
    canvas = settings.get("graph_canvas", {})
    width = canvas.get("width", settings.get("graph_width", 800))
    height = canvas.get("height", settings.get("graph_height", 200))
    max_hops = canvas.get("max_hops", settings.get("max_hops", 30))
    return width, height, max_hops

def resolve_html_dir(settings: dict) -> str:
    """
    Back-compat shim.
    Returns absolute HTML root and ensures it exists.
    New source of truth is settings['paths']['html'] with legacy fallbacks.
    """
    from pathlib import Path
    paths = resolve_all_paths(settings)
    p = Path(paths["html"]).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

def resolve_graphs_dir(settings: dict) -> str:
    """
    Back-compat shim.
    Returns absolute graphs dir and ensures it exists.
    Prefers paths.graphs; falls back to <html>/graphs.
    """
    from pathlib import Path
    paths = resolve_all_paths(settings)
    p = Path(paths["graphs"]).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

# -------------------------------
# Logging helpers
# -------------------------------
_LEVELS = {
    "CRITICAL": logging.CRITICAL, "critical": logging.CRITICAL,
    "ERROR":    logging.ERROR,    "error":    logging.ERROR,
    "WARNING":  logging.WARNING,  "warning":  logging.WARNING,
    "INFO":     logging.INFO,     "info":     logging.INFO,
    "DEBUG":    logging.DEBUG,    "debug":    logging.DEBUG,
}

def get_log_levels(settings: dict):
    """Return logging.levels (new) or legacy logging_levels."""
    return _get(settings, "logging.levels") or settings.get("logging_levels", {})

def get_log_file(settings: dict, name: str, default_filename=None):
    """Return log filename from logging.files or fallback."""
    files = _get(settings, "logging.files") or {}
    if name in files:
        return files[name]
    return default_filename or f"{name}.log"

def _resolve_level(name: str, settings: Dict[str, Any], default_level: Union[str, int] = "INFO") -> int:
    """Resolve numeric log level for a logger."""
    base = _LEVELS.get(str(default_level), logging.INFO) if not isinstance(default_level, int) else default_level
    levels = get_log_levels(settings)
    raw = levels.get(name)
    return _LEVELS.get(str(raw), base) if raw else base

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _make_file_handler(path: str, level: int) -> logging.Handler:
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    return fh

def _make_console_handler(level: int) -> logging.Handler:
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    return ch

def refresh_logger_levels(logger: logging.Logger, name: str, settings: Dict[str, Any]) -> None:
    """Re-apply the effective level from settings to an existing logger."""
    if not logger:
        return
    logger.propagate = False
    new_level = _resolve_level(name, settings, "INFO")
    logger.setLevel(logging.DEBUG)
    for h in getattr(logger, "handlers", []):
        h.setLevel(new_level)

def setup_logger(
    name: str,
    settings: Optional[Dict[str, Any]] = None,
    default_level: Union[str, int] = "INFO",
    extra_file: Optional[str] = None,
) -> logging.Logger:
    """
    Create or retrieve a logger:
    - Uses paths.logs for directory
    - Uses logging.files for filename (fallback <name>.log)
    - Honors logging.levels
    """
    settings = settings or {}
    paths = resolve_all_paths(settings)
    log_dir = paths["logs"]
    _ensure_dir(log_dir)

    log_filename = get_log_file(settings, name)
    log_path = os.path.join(log_dir, log_filename)
    extra_path = os.path.join(log_dir, extra_file) if extra_file else None

    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    effective_level = _resolve_level(name, settings, default_level)
    handler_keys = getattr(logger, "_handler_keys", set())

    # Primary handler
    pk = f"file::{log_path}"
    if pk not in handler_keys:
        logger.addHandler(_make_file_handler(log_path, effective_level))
        handler_keys.add(pk)

    # Extra handler
    if extra_path:
        ek = f"file::{extra_path}"
        if ek not in handler_keys:
            logger.addHandler(_make_file_handler(extra_path, effective_level))
            handler_keys.add(ek)

    # Console handler
    if settings.get("enable_console_logging", False):
        ck = "console::stdout"
        if ck not in handler_keys:
            logger.addHandler(_make_console_handler(effective_level))
            handler_keys.add(ck)

    logger._handler_keys = handler_keys

    # Refresh handler levels after settings reload
    for h in logger.handlers:
        h.setLevel(effective_level)

    return logger
