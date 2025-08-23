#!/usr/bin/env python3
"""
utils.py — Shared utility functions for the MTR monitoring system.

What’s new in this version
--------------------------
1) Traceroute path resolution is now *centralized* and robust:
   - Honors environment override: MTR_TRACEROUTE_DIR (if it exists)
   - Uses settings['paths']['traceroute'] (preferred)
   - Falls back to legacy settings['paths']['traces'] (older configs)
   - If neither exists, tries common defaults:
       /opt/scripts/MTR_WEB/traceroute
       /opt/scripts/MTR_WEB/traces
   - Logs the final choice via your configured logging.

2) Logging continues to use your YAML-driven levels and files.
3) All functions documented for users with basic Python knowledge.
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
    """
    Load the YAML settings file.

    If settings_path is None, we look for ../mtr_script_settings.yaml
    relative to this file’s directory.
    """
    if settings_path is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        settings_path = os.path.join(base_dir, "mtr_script_settings.yaml")
    with open(settings_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def repo_root() -> str:
    """
    Return the absolute path to the repository root.
    (Two levels up from this file.)
    """
    return str(Path(__file__).resolve().parents[2])

# -------------------------------
# Path resolution
# -------------------------------

def _get(settings: dict, dotted: str, default=None):
    """Fetch a nested key using dot notation, e.g. 'paths.html'."""
    cur = settings
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def _dir_exists(p: Optional[str]) -> bool:
    """True if p is a non-empty string and an existing directory."""
    try:
        return bool(p) and os.path.isdir(p)
    except Exception:
        return False

def get_path(settings: dict, key: str, legacy_keys=None, default=None) -> str:
    """
    Resolve a path from settings['paths'][key] with optional legacy fallbacks.
    Returns the value as a str (no existence check).
    """
    val = _get(settings, f"paths.{key}")
    if val:
        return str(val)
    if legacy_keys:
        for k in legacy_keys:
            if k in settings and settings[k]:
                return str(settings[k])
    return default

def resolve_all_paths(settings: dict) -> Dict[str, str]:
    """
    Build a canonical paths dict consumed by the rest of the code.

    Keys returned:
      - html, rrd, logs, graphs
      - traceroute  (canonical; see resolution order below)
      - cache       (optional; may be None — code using it will fallback)

    Traceroute resolution order (first existing directory wins):
      1) env MTR_TRACEROUTE_DIR
      2) settings.paths.traceroute
      3) settings.paths.traces         # legacy key
      4) /opt/scripts/MTR_WEB/traceroute
      5) /opt/scripts/MTR_WEB/traces

    A log line is emitted to show which traceroute directory is in use.
    """
    paths_cfg = (settings or {}).get("paths", {}) or {}
    logger = setup_logger("paths", settings=settings)

    html_dir = paths_cfg.get("html") or "/opt/scripts/MTR_WEB/html"
    rrd_dir  = paths_cfg.get("rrd")  or "/opt/scripts/MTR_WEB/data"
    logs_dir = paths_cfg.get("logs") or "/opt/scripts/MTR_WEB/logs"
    graphs_dir = paths_cfg.get("graphs") or (str(Path(html_dir) / "graphs"))
    cache_dir  = paths_cfg.get("cache")  # may be None; callers will fallback if needed

    # ---- Traceroute resolution (normalized) ----
    env_tr     = os.environ.get("MTR_TRACEROUTE_DIR")
    tr_yaml    = paths_cfg.get("traceroute")
    tr_legacy  = paths_cfg.get("traces")   # legacy key

    candidates = []
    if env_tr:    candidates.append(("env:MTR_TRACEROUTE_DIR", env_tr))
    if tr_yaml:   candidates.append(("settings.paths.traceroute", tr_yaml))
    if tr_legacy: candidates.append(("settings.paths.traces", tr_legacy))
    # sensible defaults last
    candidates.extend([
        ("default:traceroute", "/opt/scripts/MTR_WEB/traceroute"),
        ("default:traces",     "/opt/scripts/MTR_WEB/traces"),
    ])

    traceroute_dir = None
    chosen_tag = None
    for tag, d in candidates:
        if _dir_exists(d):
            traceroute_dir, chosen_tag = d, tag
            break

    if traceroute_dir:
        logger.info(f"Using traceroute dir ({chosen_tag}): {traceroute_dir}")
    else:
        logger.warning("No usable traceroute path found; hop labels will be empty and 'varies' cannot update.")

    # Ensure base dirs exist
    for p in (html_dir, rrd_dir, logs_dir, graphs_dir):
        os.makedirs(p, exist_ok=True)

    return {
        "html":       html_dir,
        "rrd":        rrd_dir,
        "logs":       logs_dir,
        "graphs":     graphs_dir,
        "traceroute": traceroute_dir,  # canonical key everyone else should use
        "cache":      cache_dir,       # optional
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
    """
    Return HTML knobs: (auto_refresh_seconds, log_lines_display),
    honoring both new and legacy keys.
    """
    html = settings.get("html", {})
    auto_refresh = html.get("auto_refresh_seconds", settings.get("html_auto_refresh_seconds", 0))
    log_lines_display = html.get("log_lines_display", settings.get("log_lines_display", 50))
    return auto_refresh, log_lines_display

def resolve_canvas(settings: dict):
    """
    Return graph canvas (width, height, max_hops) with legacy fallback.
    """
    canvas = settings.get("graph_canvas", {})
    width = canvas.get("width", settings.get("graph_width", 800))
    height = canvas.get("height", settings.get("graph_height", 200))
    max_hops = canvas.get("max_hops", settings.get("max_hops", 30))
    return width, height, max_hops

def resolve_html_dir(settings: dict) -> str:
    """
    Back‑compat shim. Returns absolute HTML root and ensures it exists.
    The source of truth is settings['paths']['html'].
    """
    p = Path(resolve_all_paths(settings)["html"]).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

def resolve_graphs_dir(settings: dict) -> str:
    """
    Back‑compat shim. Returns absolute graphs dir and ensures it exists.
    Prefers settings['paths']['graphs']; falls back to <html>/graphs.
    """
    p = Path(resolve_all_paths(settings)["graphs"]).resolve()
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
    """Return log filename from logging.files or fallback to <name>.log."""
    files = _get(settings, "logging.files") or {}
    if name in files:
        return files[name]
    return default_filename or f"{name}.log"

def _resolve_level(name: str, settings: Dict[str, Any], default_level: Union[str, int] = "INFO") -> int:
    """Resolve numeric log level for a logger based on settings."""
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
    """
    Re-apply the effective level from settings to an existing logger.
    Useful after live reload of settings.
    """
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
    Create or retrieve a logger named `name`.

    - Log directory comes from paths.logs
    - Filename comes from logging.files[name], fallback <name>.log
    - Level comes from logging.levels[name], fallback default_level
    - Optional second file handler if extra_file is provided.
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

    # Console handler (optional)
    if settings.get("enable_console_logging", False):
        ck = "console::stdout"
        if ck not in handler_keys:
            logger.addHandler(_make_console_handler(effective_level))
            handler_keys.add(ck)

    logger._handler_keys = handler_keys

    # Ensure all handlers reflect the final level
    for h in logger.handlers:
        h.setLevel(effective_level)

    return logger
