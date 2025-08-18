#!/usr/bin/env python3
import os
import time
import rrdtool
import logging
from datetime import datetime

def _get_logger(logger):
    # If caller didn't pass a logger, use the shared 'rrd' logger name
    return logger if logger is not None else logging.getLogger("rrd")

def init_rrd(rrd_path, settings, logger):
    """
    Creates a single RRD file to store all hop metrics in one place (optional).
    NOTE: DS names start at hop1_* (no hop0).
    """
    logger = _get_logger(logger)
    
    if os.path.exists(rrd_path):
        return

    rrd_config = settings.get("rrd", {})
    step = rrd_config.get("step", 60)
    heartbeat = rrd_config.get("heartbeat", 120)
    ds_schema = rrd_config.get("data_sources", [])
    rra_schema = rrd_config.get("rras", [])
    max_hops = int(settings.get("max_hops", 30))

    data_sources = []
    # --- start at hop 1 (ban hop0) ---
    for i in range(1, max_hops + 1):
        for ds in ds_schema:
            name = f"hop{i}_{ds['name']}"  # e.g., hop1_avg
            data_sources.append(
                f"DS:{name}:{ds['type']}:{heartbeat}:{ds['min']}:{ds['max']}"
            )

    rras = [
        f"RRA:{r['cf']}:{r['xff']}:{r['step']}:{r['rows']}"
        for r in rra_schema
    ]

    rrdtool.create(rrd_path, "--step", str(step), *data_sources, *rras)
    logger.info(f"[{rrd_path}] RRD created with hop1..hop{max_hops} schema.")


def init_per_hop_rrds(ip, settings, logger):
    """
    Creates separate RRD files per hop (e.g., 1.1.1.1_hop1.rrd, hop2.rrd, ...).
    NOTE: starts at hop1 (no hop0 files).
    """
    logger = _get_logger(logger)
    rrd_config = settings.get("rrd", {})
    step = rrd_config.get("step", 60)
    heartbeat = rrd_config.get("heartbeat", 120)
    ds_schema = rrd_config.get("data_sources", [])
    rra_schema = rrd_config.get("rras", [])
    rrd_dir = settings.get("rrd_directory", "rrd")
    max_hops = int(settings.get("max_hops", 30))

    os.makedirs(rrd_dir, exist_ok=True)

    for hop in range(1, max_hops + 1):  # --- start at 1 ---
        hop_rrd_path = os.path.join(rrd_dir, f"{ip}_hop{hop}.rrd")

        if os.path.exists(hop_rrd_path):
            continue

        data_sources = [
            f"DS:{ds['name']}:{ds['type']}:{heartbeat}:{ds['min']}:{ds['max']}"
            for ds in ds_schema
        ]
        rras = [
            f"RRA:{r['cf']}:{r['xff']}:{r['step']}:{r['rows']}"
            for r in rra_schema
        ]

        rrdtool.create(hop_rrd_path, "--step", str(step), *data_sources, *rras)
        logger.info(f"[{hop_rrd_path}] Per-hop RRD created.")


def update_rrd(rrd_path, hops, ip, settings, debug_log=True):
    """
    Updates the single multi-hop RRD (if used) AND each per-hop RRD.

    We strictly use hop numbers >= 1 (MTR's 'count'); hop0 is ignored.
    """
    logger = _get_logger(logger)
    rrd_config = settings.get("rrd", {})
    ds_schema = rrd_config.get("data_sources", [])
    rrd_dir = settings.get("rrd_directory", "rrd")
    max_hops = int(settings.get("max_hops", 30))

    # Build a quick lookup from hop_num -> hop dict
    by_index = {}
    for h in hops:
        try:
            n = int(h.get("count", 0))
        except (TypeError, ValueError):
            continue
        if n >= 1:
            by_index[n] = h

    # ---- Update the single multi-hop RRD (if present) ----
    if os.path.exists(rrd_path):
        values = []
        for i in range(1, max_hops + 1):
            hop = by_index.get(i, {})
            # read metrics defensively
            def fget(key):
                v = hop.get(key)
                try:
                    return round(float(v), 2)
                except Exception:
                    return 'U'
            avg  = fget("Avg")
            last = fget("Last")
            best = fget("Best")
            loss = fget("Loss%")
            values += [avg, last, best, loss]

        timestamp = int(time.time())
        update_str = f"{timestamp}:" + ":".join(str(v) for v in values)
        try:
            rrdtool.update(rrd_path, update_str)
        except rrdtool.OperationalError as e:
            logger.error(f"[RRD ERROR] {e}")

        if debug_log:
            with open(debug_log if isinstance(debug_log, str) else os.path.join(rrd_dir, "rrd_debug.log"), "a") as f:
                f.write(f"{datetime.now()} {ip} (multi) values: {values}\n")

    # ---- Update each per-hop RRD ----
    for i in range(1, max_hops + 1):
        hop_rrd_path = os.path.join(rrd_dir, f"{ip}_hop{i}.rrd")
        if not os.path.exists(hop_rrd_path):
            # If a specific hop didn't exist yet (new higher hop), try to create on the fly
            init_per_hop_rrds(ip, settings, logger)
            if not os.path.exists(hop_rrd_path):
                continue

        hop = by_index.get(i, {})
        fields = []
        for ds in ds_schema:
            key = ds["name"]
            v = hop.get("Avg") if key == "avg" else \
                hop.get("Last") if key == "last" else \
                hop.get("Best") if key == "best" else \
                hop.get("Loss%") if key in ("loss", "loss_pct", "loss%") else None
            try:
                fields.append(str(round(float(v), 2)))
            except Exception:
                fields.append("U")

        ts = int(time.time())
        try:
            rrdtool.update(hop_rrd_path, f"{ts}:" + ":".join(fields))
        except rrdtool.OperationalError as e:
            logger.error(f"[RRD ERROR] {e}"))
        if debug_log:
            with open(debug_log if isinstance(debug_log, str) else os.path.join(rrd_dir, "rrd_debug.log"), "a") as f:
                f.write(f"{datetime.now()} {ip} hop{i} fields: {fields}\n")
