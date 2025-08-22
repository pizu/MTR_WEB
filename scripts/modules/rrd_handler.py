#!/usr/bin/env python3
"""
rrd_handler.py
---------------
RRD helper functions for the MTR monitoring pipeline.

Overview
--------
This module now uses a **single, multi-hop RRD per target** and writes all hop
data into that one file using multiple DS entries (hop1_avg, hop1_last, ...).
**Per-hop RRD files have been removed** to reduce disk I/O and file churn.

Where settings come from (YAML)
-------------------------------
settings['rrd_directory']   -> base directory for RRDs (defaults to "data")
settings['max_hops']        -> number of hop slots to provision in the multi-hop file (default 30)
settings['rrd']             -> dict with:
  step        : int    (e.g., 60)
  heartbeat   : int    (e.g., 120)
  data_sources: list of { name, type, min, max }        # e.g. avg,last,best,loss
  rras        : list of { cf, xff, step, rows }

Typical DS schema in YAML:
  rrd:
    data_sources:
      - { name: "avg",  type: "GAUGE", min: 0,  max: "U" }
      - { name: "last", type: "GAUGE", min: 0,  max: "U" }
      - { name: "best", type: "GAUGE", min: 0,  max: "U" }
      - { name: "loss", type: "GAUGE", min: 0,  max: 100 }

Logging
-------
Uses the provided logger or falls back to logging.getLogger("rrd").
No prints; errors are logged.

Python compatibility
--------------------
Python 3.7+.
"""

import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import rrdtool  # type: ignore
except Exception as e:
    raise RuntimeError(f"rrdtool module is required: {e}")


# ---------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------

def _get_logger(logger: Optional[logging.Logger]) -> logging.Logger:
    """Return provided logger or a shared 'rrd' logger."""
    return logger if logger is not None else logging.getLogger("rrd")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _rrd_cfg(settings: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Return the settings['rrd'] subdict or {}."""
    return (settings or {}).get("rrd", {}) or {}


def _rrd_dir(settings: Optional[Dict[str, Any]]) -> str:
    """Return the target directory for RRD files (default: 'data')."""
    base = (settings or {}).get("rrd_directory")
    return base if base else "data"


def _rras_from_settings(settings: Optional[Dict[str, Any]]) -> List[str]:
    """
    Build RRA directives for rrdtool.create() from settings['rrd']['rras'].
    Each item supports: cf (AVERAGE|MAX|MIN|LAST), xff (0..1), step (PDPs/row), rows.
    """
    rras_cfg = _rrd_cfg(settings).get("rras")
    rras: List[str] = []
    if isinstance(rras_cfg, list) and rras_cfg:
        for r in rras_cfg:
            try:
                cf   = str(r.get("cf", "AVERAGE")).upper()
                xff  = float(r.get("xff", 0.5))
                step = int(r.get("step", 1))
                rows = int(r.get("rows", 4320))
                rras.append(f"RRA:{cf}:{xff}:{step}:{rows}")
            except Exception:
                # Skip malformed entries silently
                continue
    else:
        # Sane defaults if none are configured
        rras.extend([
            "RRA:AVERAGE:0.5:1:4320",   # ~3 days at 'step'
            "RRA:AVERAGE:0.5:5:4032",  # ~2 weeks at 5*step
            "RRA:MAX:0.5:5:4032",
        ])
    return rras


def _create_rrd(path: str, settings: Optional[Dict[str, Any]], ds_schema: List[Dict[str, Any]], logger: Optional[logging.Logger]) -> None:
    """
    Create an RRD file with the given DS schema and RRAs. Idempotent.
    """
    logger = _get_logger(logger)
    if os.path.exists(path):
        return

    cfg       = _rrd_cfg(settings)
    step      = int(cfg.get("step", 60))
    heartbeat = int(cfg.get("heartbeat", step * 2))

    _ensure_dir(os.path.dirname(path))

    # Build DS lines (accept 'U' for unknown bounds as strings)
    ds_lines: List[str] = []
    for ds in (ds_schema or []):
        name = str(ds.get("name", "")).strip()
        dtype = str(ds.get("type", "GAUGE")).strip()
        dmin = ds.get("min", "U")
        dmax = ds.get("max", "U")
        if not name:
            continue
        ds_lines.append(f"DS:{name}:{dtype}:{heartbeat}:{dmin}:{dmax}")

    rras = _rras_from_settings(settings)

    try:
        rrdtool.create(
            path,
            "--step", str(step),
            *ds_lines,
            *rras
        )
        logger.info(f"[RRD] created {path} (step={step}, heartbeat={heartbeat})")
    except Exception as e:
        logger.error(f"[RRD] create failed for {path}: {e}")


def _float_or_U(v: Any) -> str:
    """
    Convert a value to a stringed float rounded to 2 decimals, else 'U'.
    """
    try:
        return str(round(float(v), 2))
    except Exception:
        return "U"


# ---------------------------------------------------------------------
# Public API (single multi-hop file; per-hop files removed)
# ---------------------------------------------------------------------

def init_rrd(rrd_path: str, settings: Dict[str, Any], logger: Optional[logging.Logger]) -> None:
    """
    Create ONE multi-hop RRD for a target.
    DS names are constructed as hop{N}_{ds_name} for N=1..max_hops, using the
    DS list found in settings['rrd']['data_sources'].

    NOTE: Hop indices start at 1; there is no hop0.
    """
    logger = _get_logger(logger)
    if os.path.exists(rrd_path):
        return

    cfg       = _rrd_cfg(settings)
    step      = int(cfg.get("step", 60))
    heartbeat = int(cfg.get("heartbeat", step * 2))
    ds_schema = cfg.get("data_sources", []) or []
    rras      = _rras_from_settings(settings)
    max_hops  = int((settings or {}).get("max_hops", 30))

    ds_lines: List[str] = []
    for i in range(1, max_hops + 1):  # start at hop1
        for ds in ds_schema:
            name = f"hop{i}_{ds.get('name')}"
            dtype = ds.get("type", "GAUGE")
            dmin = ds.get("min", "U")
            dmax = ds.get("max", "U")
            ds_lines.append(f"DS:{name}:{dtype}:{heartbeat}:{dmin}:{dmax}")

    _ensure_dir(os.path.dirname(rrd_path))
    try:
        rrdtool.create(rrd_path, "--step", str(step), *ds_lines, *rras)
        logger.info(f"[RRD] created (multi) {rrd_path} with hop1..hop{max_hops}")
    except Exception as e:
        logger.error(f"[RRD] create failed for (multi) {rrd_path}: {e}")


def update_rrd(rrd_path: str,
               hops: List[Dict[str, Any]],
               ip: str,
               settings: Optional[Dict[str, Any]],
               debug_log: Any = True,
               logger: Optional[logging.Logger] = None) -> None:
    """
    Update the single multi-hop RRD (if it exists).

    Parameters
    ----------
    rrd_path  : str
        Path to the *single* (multi-hop) RRD file. If it does not exist, we skip it.
    hops      : list of dicts
        Parsed MTR report. Each hop dict may include keys like:
          'count', 'host', 'Loss%', 'Last', 'Avg', 'Best', ...
    ip        : str
        Target IP/host (unused here but kept for signature compatibility).
    settings  : dict or None
        YAML settings. If None, defaults are used.
    debug_log : one of [False, True, <path-to-file>, <path-to-directory>]
        - False/None : no debug text log
        - True       : append to <rrd_dir>/rrd_debug.log
        - str (file) : append to that file path
        - str (dir)  : append to <dir>/rrd_debug.log
    logger    : logging.Logger or None
        Logger to use; defaults to logging.getLogger("rrd")
    """
    logger = _get_logger(logger)
    cfg       = _rrd_cfg(settings)
    ds_schema = cfg.get("data_sources", []) or []
    rrd_dir   = _rrd_dir(settings)
    max_hops  = int((settings or {}).get("max_hops", 30))

    _ensure_dir(rrd_dir)

    # Build a quick lookup: hop index -> hop dict (ignore hop0 if seen)
    by_index: Dict[int, Dict[str, Any]] = {}
    for h in (hops or []):
        try:
            n = int(h.get("count", 0))
        except Exception:
            continue
        if n >= 1:
            by_index[n] = h

    # Resolve the debug log path if any
    if debug_log:
      logger.debug(f"[{ip}] (multi) values: {values}")

    # ------------------------------
    # Update the (single) multi-hop RRD if it exists
    # ------------------------------
    if rrd_path and os.path.exists(rrd_path):
        values: List[str] = []
        # The order of values must match how the DS were created in init_rrd()
        for i in range(1, max_hops + 1):
            hop = by_index.get(i, {})
            avg  = _float_or_U(hop.get("Avg"))
            last = _float_or_U(hop.get("Last"))
            best = _float_or_U(hop.get("Best"))
            loss = _float_or_U(hop.get("Loss%"))
            values += [avg, last, best, loss]

        ts = int(time.time())
        update_str = f"{ts}:{':'.join(values)}"
        try:
            rrdtool.update(rrd_path, update_str)
        except rrdtool.OperationalError as e:
            logger.error(f"[RRD ERROR] multi-hop update failed for {rrd_path}: {e}")
        except Exception as e:
            logger.error(f"[RRD ERROR] multi-hop update unexpected error for {rrd_path}: {e}")

        if debug_log:
          logger.debug(f"[{ip}] (multi) values: {values}")
