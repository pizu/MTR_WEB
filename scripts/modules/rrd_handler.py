#!/usr/bin/env python3
"""
rrd_handler.py
---------------
RRD helper functions for the MTR monitoring pipeline.

Key updates in this version:
- Unified paths via modules.utils.resolve_all_paths(settings); RRDs live under settings['paths']['rrd'].
- DS creation is driven entirely by settings['rrd']['data_sources'] so adding/removing metrics is schema-driven.
- Update logic no longer hardcodes ['avg','last','best','loss']; it respects the DS order from settings and
  pulls values from MTR hop dictionaries using a flexible field map, including 'StDev' → 'stdev' support.
- Hop indices start at 1 (there is no hop0).

Expected DS schema example in mtr_script_settings.yaml:

rrd:
  step: 60
  heartbeat: 120
  data_sources:
    - { name: "avg",   type: "GAUGE", min: 0,   max: "U" }
    - { name: "last",  type: "GAUGE", min: 0,   max: "U" }
    - { name: "best",  type: "GAUGE", min: 0,   max: "U" }
    - { name: "loss",  type: "GAUGE", min: 0,   max: 100 }
    - { name: "stdev", type: "GAUGE", min: 0,   max: "U" }   # <-- add this to store jitter-like variance

The exporter maps 'stdev' → JSON key 'varies' so the UI shows “Varies (ms)”.
"""

import os
import time
import logging
from typing import Any, Dict, List, Optional

from modules.utils import resolve_all_paths  # unified path resolver

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
    """
    Return the target directory for RRD files.
    Prefer settings['paths']['rrd'] via resolve_all_paths(); fallback to legacy key.
    """
    try:
        paths = resolve_all_paths(settings or {})
        base = paths.get("rrd")
        if base:
            return base
    except Exception:
        pass
    # Legacy fallback (kept for backwards compatibility)
    base = (settings or {}).get("rrd_directory")
    return base if base else "data"

def _rras_from_settings(settings: Optional[Dict[str, Any]]) -> List[str]:
    """
    Build RRA directives for rrdtool.create() from settings['rrd']['rras'].
    Each item supports keys: cf (AVERAGE|MAX|MIN|LAST), xff (0..1), step (PDPs/row), rows.
    Falls back to sane defaults if not provided.
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
                continue
    else:
        # Defaults (AVERAGE short + medium, MAX medium)
        rras.extend([
            "RRA:AVERAGE:0.5:1:4320",   # ~3 days at 'step'
            "RRA:AVERAGE:0.5:5:4032",  # ~2 weeks at 5*step
            "RRA:MAX:0.5:5:4032",
        ])
    return rras

def _float_or_U(v: Any) -> str:
    """Convert value to a rounded string or 'U' if not a number."""
    try:
        return str(round(float(v), 3))
    except Exception:
        return "U"

# Map DS names → the MTR JSON field names we expect on each hop dict
# We accept multiple aliases for robustness.
MTR_FIELD_MAP: Dict[str, List[str]] = {
    # DS name  -> possible hop keys (first found wins)
    "avg":   ["Avg", "avg"],
    "last":  ["Last", "last"],
    "best":  ["Best", "best"],
    "loss":  ["Loss%", "loss", "Loss"],
    "stdev": ["StDev", "stdev", "var", "variance", "jitter"],  # StDev in mtr --json
    # If someone defines a DS named 'varies' we still try to feed it from StDev:
    "varies": ["StDev", "stdev", "var", "variance", "jitter"],
}

def _extract_hop_value(hop: Dict[str, Any], ds_name: str) -> str:
    """
    Extract a numeric value for the given DS name from a hop dict using MTR_FIELD_MAP.
    Returns a string suitable for rrdtool.update(), i.e., number or 'U'.
    """
    for key in MTR_FIELD_MAP.get(ds_name, [ds_name]):
        if key in hop:
            return _float_or_U(hop.get(key))
    # As a last resort, try exact ds_name
    return _float_or_U(hop.get(ds_name))

# ---------------------------------------------------------------------
# Public API (single multi-hop file; per-hop files removed)
# ---------------------------------------------------------------------

def init_rrd(rrd_path: str, settings: Dict[str, Any], logger: Optional[logging.Logger]) -> None:
    """
    Create ONE multi-hop RRD for a target.
    DS names are constructed as hop{N}_{ds_name} for N=1..max_hops using the DS list in
    settings['rrd']['data_sources'].

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

    # Build DS lines strictly in schema order; exporter/update rely on this order.
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
        logger.info(f"[RRD] created {rrd_path} with hop1..hop{max_hops} and DS {[d.get('name') for d in ds_schema]}")
    except Exception as e:
        logger.error(f"[RRD] create failed for {rrd_path}: {e}")

def update_rrd(rrd_path: str,
               hops: List[Dict[str, Any]],
               ip: str,
               settings: Optional[Dict[str, Any]],
               debug_log: bool = False,
               logger: Optional[logging.Logger] = None) -> None:
    """
    Update the single multi-hop RRD (if it exists).

    Parameters
    ----------
    rrd_path : str
        Path to the per-target RRD file (multi-hop schema).
    hops : List[Dict]
        List of hop dicts from mtr --json, each including at minimum:
          - 'count' (1..N)
          - 'Avg', 'Last', 'Best', 'Loss%', and optionally 'StDev'
    ip : str
        Target IP (for logging).
    settings : dict
        Full settings dict; must include rrd.data_sources and max_hops.
    debug_log : bool
        If True, logs the flattened values list.
    logger : logging.Logger
        Optional existing logger.

    Behavior
    --------
    - We respect the DS order from settings['rrd']['data_sources'].
    - For each hop i in [1..max_hops], for each DS in schema, we find the corresponding hop value
      using MTR_FIELD_MAP and append it to the update string. Missing => 'U'.
    """
    logger = _get_logger(logger)
    cfg       = _rrd_cfg(settings)
    ds_schema = cfg.get("data_sources", []) or []
    rrd_dir   = _rrd_dir(settings)  # ensure dir exists
    max_hops  = int((settings or {}).get("max_hops", 30))

    _ensure_dir(rrd_dir)

    # Index hops by 'count' (ignore hop0, if present)
    by_index: Dict[int, Dict[str, Any]] = {}
    for h in (hops or []):
        try:
            n = int(h.get("count", 0))
        except Exception:
            continue
        if n >= 1:
            by_index[n] = h

    # Update only if target RRD exists (creator should have been called at provisioning time)
    if not (rrd_path and os.path.exists(rrd_path)):
        logger.warning(f"[RRD] update skipped; file missing for {ip}: {rrd_path}")
        return

    # Flatten values: hop-major, DS-minor; order MUST match init_rrd() DS order
    values: List[str] = []
    for i in range(1, max_hops + 1):
        hop = by_index.get(i, {})  # may be {}
        for ds in ds_schema:
            ds_name = str(ds.get("name"))
            values.append(_extract_hop_value(hop, ds_name))

    ts = int(time.time())
    update_str = f"{ts}:{':'.join(values)}"
    try:
        rrdtool.update(rrd_path, update_str)
    except rrdtool.OperationalError as e:
        logger.error(f"[RRD ERROR] update failed for {rrd_path}: {e}")
    except Exception as e:
        logger.error(f"[RRD ERROR] unexpected error for {rrd_path}: {e}")

    if debug_log:
        logger.debug(f"[{ip}] values: {values}")
