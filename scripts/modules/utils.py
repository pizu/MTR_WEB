#!/usr/bin/env python3
"""
utils.py - Shared utility functions for the MTR monitoring system.

This module provides:
- load_settings(): Load settings from mtr_script_settings.yaml
- setup_logger(): Standardized logger initialization for scripts
- refresh_logger_levels(): Re-apply levels to an existing logger + handlers

Expected keys in mtr_script_settings.yaml:

log_directory: "/opt/scripts/MTR_WEB/logs/"
interval_seconds: 60
loss_threshold: 10
debounce_seconds: 300
retention_days: 30
log_lines_display: 100

logging_levels:
  controller: INFO
  mtr_monitor: DEBUG
  graph_generator: WARNING
  html_generator: INFO
  index_generator: INFO

# Optional:
enable_console_logging: false   # default false; set true for foreground runs
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any

# -------------------------------
# Settings helpers
# -------------------------------
def load_settings(settings_path: str | None = None) -> Dict[str, Any]:
    """
    Load YAML settings. If path is None, assume repo_root/mtr_script_settings.yaml.
    """
    if settings_path is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        settings_path = os.path.join(base_dir, "mtr_script_settings.yaml")
    with open(settings_path, 'r') as f:
        return yaml.safe_load(f) or {}

def repo_root() -> str:
    """
    Absolute path to the repo root.
    utils.py lives in: <repo>/scripts/modules/utils.py
    So repo root = parents[2].
    """
    return str(Path(__file__).resolve().parents[2])

def resolve_html_dir(settings: dict) -> str:
    """
    Resolve the website root where index/<ip>.html, data/, graphs/ live.
    - settings['html_directory'] may be absolute or relative to repo root.
    Ensures the directory exists.
    """
    root = Path(repo_root())
    html_dir = settings.get("html_directory", "html")
    p = Path(html_dir)
    if not p.is_absolute():
        p = root / html_dir
    p = p.resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

def resolve_graphs_dir(settings: dict) -> str:
    """
    Graphs directory. If graph_output_directory is not set,
    defaults to <html_dir>/graphs. Ensures the directory exists.
    """
    override = settings.get("graph_output_directory")
    if override:
        p = Path(override)
        if not p.is_absolute():
            p = Path(repo_root()) / override
    else:
        p = Path(resolve_html_dir(settings)) / "graphs"
    p = p.resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

def resolve_targets_path() -> str:
    """Absolute path to mtr_targets.yaml at repo root."""
    return str(Path(repo_root()) / "mtr_targets.yaml")

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

def _resolve_level(name: str, settings: Dict[str, Any], default_level: str | int = "INFO") -> int:
    """
    Resolve an integer log level for a given logger name from settings.logging_levels.
    Falls back to default_level (string or int).
    """
    lvl = default_level
    if isinstance(default_level, int):
        lvl = default_level
    else:
        lvl = _LEVELS.get(str(default_level), logging.INFO)

    levels = (settings or {}).get("logging_levels", {}) or {}
    raw = levels.get(name, None)
    if raw is None:
        return lvl
    return _LEVELS.get(str(raw), lvl)

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _make_file_handler(path: str, level: int) -> logging.Handler:
    """
    Simple file handler (no rotation here; you can swap to TimedRotatingFileHandler if desired).
    """
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
    Re-apply the effective level from settings to the logger and ALL its handlers.
    Safe to call after every settings reload without adding duplicate handlers.
    """
    if logger is None:
        return
    # Make sure nothing bubbles to root (root may be INFO)
    logger.propagate = False

    new_level = _resolve_level(name, settings, default_level="INFO")
    # Accept everything in the logger; handlers do the filtering
    logger.setLevel(logging.DEBUG)

    # Update existing handlers to the new level
    for h in getattr(logger, "handlers", []):
        h.setLevel(new_level)

def setup_logger(
    name: str,
    log_directory: str,
    log_filename: str,
    settings: Dict[str, Any] | None = None,
    default_level: str | int = "INFO",
    extra_file: str | None = None,
) -> logging.Logger:
    """
    Create or retrieve a logger:
    - No propagation to root (prevents unintended INFO from root handlers)
    - On every call, (re)apply the configured level to logger + all handlers
    - Idempotent handler creation (won't duplicate handlers)
    - Optional per-target extra file handler
    - Optional console handler via settings['enable_console_logging'] (default False)
    """
    _ensure_dir(log_directory)
    logger = logging.getLogger(name)
    logger.propagate = False

    # Always set logger to capture all; handlers decide filtering
    logger.setLevel(logging.DEBUG)

    # Resolve current effective level from settings
    effective_level = _resolve_level(name, settings or {}, default_level=default_level)

    # Build stable file paths
    primary_path = os.path.join(log_directory, log_filename)
    extra_path = os.path.join(log_directory, extra_file) if extra_file else None

    # Track handler keys to avoid duplicates across repeated calls
    handler_keys = getattr(logger, "_handler_keys", set())

    # Primary file handler
    primary_key = f"file::{primary_path}"
    if primary_key not in handler_keys:
        logger.addHandler(_make_file_handler(primary_path, effective_level))
        handler_keys.add(primary_key)

    # Optional per-target handler
    if extra_path:
        extra_key = f"file::{extra_path}"
        if extra_key not in handler_keys:
            logger.addHandler(_make_file_handler(extra_path, effective_level))
            handler_keys.add(extra_key)

    # Optional console handler (default off for services/cron)
    enable_console = bool((settings or {}).get("enable_console_logging", False))
    console_key = "console::stdout"
    if enable_console and console_key not in handler_keys:
        logger.addHandler(_make_console_handler(effective_level))
        handler_keys.add(console_key)

    # Save keys back for future idempotent calls
    logger._handler_keys = handler_keys

    # IMPORTANT: Even if handlers already existed from a previous call,
    # re-apply the current level so YAML changes take effect immediately.
    for h in logger.handlers:
        h.setLevel(effective_level)

    return logger
