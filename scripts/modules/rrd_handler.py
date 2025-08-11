#!/usr/bin/env python3
import os
import time
import rrdtool
from datetime import datetime

def init_rrd(rrd_path, settings, logger):
    """
    Creates a single RRD file to store all hop metrics in one place.

    Arguments:
    - rrd_path: Path to the RRD file (e.g., 'rrd/1.1.1.1.rrd')
    - settings: Dictionary loaded from mtr_script_settings.yaml
    - logger: Logger object to log creation info or errors
    """

    if os.path.exists(rrd_path):
        return  # Do not recreate if file already exists

    # Get RRD configuration from settings
    rrd_config = settings.get("rrd", {})
    step = rrd_config.get("step", 60)               # Interval between samples
    heartbeat = rrd_config.get("heartbeat", 120)    # Max time allowed between updates
    ds_schema = rrd_config.get("data_sources", [])  # List of metrics to track
    rra_schema = rrd_config.get("rras", [])         # How long and at what resolution to store data

    data_sources = []
    max_hops = settings.get("max_hops", 30)

    # Define metrics for each hop (avg, loss, best, etc.)
    for i in range(0, max_hops + 1):
        for ds in ds_schema:
            name = f"hop{i}_{ds['name']}"  # e.g., hop0_avg
            data_sources.append(
                f"DS:{name}:{ds['type']}:{heartbeat}:{ds['min']}:{ds['max']}"
            )

    # Define data retention rules
    rras = [
        f"RRA:{r['cf']}:{r['xff']}:{r['step']}:{r['rows']}"
        for r in rra_schema
    ]

    # Create the RRD file
    rrdtool.create(rrd_path, "--step", str(step), *data_sources, *rras)
    logger.info(f"[{rrd_path}] RRD created with dynamic schema.")


def init_per_hop_rrds(ip, settings, logger):
    """
    Creates separate RRD files per hop (e.g., 1.1.1.1_hop0.rrd, hop1.rrd, etc.)

    Arguments:
    - ip: The target IP address
    - settings: Configuration loaded from YAML
    - logger: Logger object
    """

    rrd_config = settings.get("rrd", {})
    step = rrd_config.get("step", 60)
    heartbeat = rrd_config.get("heartbeat", 120)
    ds_schema = rrd_config.get("data_sources", [])
    rra_schema = rrd_config.get("rras", [])
    rrd_dir = settings.get("rrd_directory", "rrd")
    max_hops = settings.get("max_hops", 30)

    os.makedirs(rrd_dir, exist_ok=True)

    for hop in range(max_hops + 1):
        hop_rrd_path = os.path.join(rrd_dir, f"{ip}_hop{hop}.rrd")

        if os.path.exists(hop_rrd_path):
            continue  # Already created

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
    Updates a multi-hop RRD file with new metrics from the latest MTR run.

    Arguments:
    - rrd_path: Path to the RRD file (e.g., rrd/1.1.1.1.rrd)
    - hops: List of dictionaries with hop data
    - ip: The monitored target IP
    - settings: YAML configuration
    - debug_log: Optional path to log values for troubleshooting
    """

    values = []
    max_hops = settings.get("max_hops", 30)

    for i in range(0, max_hops + 1):
        hop = next((h for h in hops if h.get("count") == i), {})

        # Try to read and format each metric
        try:
            avg = round(float(hop.get("Avg", 'U')), 2) if hop.get("Avg") not in [None, 'U'] else 'U'
        except:
            avg = 'U'

        try:
            last = round(float(hop.get("Last", 'U')), 2) if hop.get("Last") not in [None, 'U'] else 'U'
        except:
            last = 'U'

        try:
            best = round(float(hop.get("Best", 'U')), 2) if hop.get("Best") not in [None, 'U'] else 'U'
        except:
            best = 'U'

        try:
            loss = round(float(hop.get("Loss%", 'U')), 2) if hop.get("Loss%") not in [None, 'U'] else 'U'
        except:
            loss = 'U'

        # Add this hop's values to the full list
        values += [avg, last, best, loss]

    # Combine into RRD update format
    timestamp = int(time.time())
    update_str = f"{timestamp}:" + ":".join(str(v) for v in values)

    try:
        rrdtool.update(rrd_path, update_str)
    except rrdtool.OperationalError as e:
        print(f"[RRD ERROR] {e}")

    if debug_log:
        with open(debug_log, "a") as f:
            f.write(f"{datetime.now()} {ip} values: {values}\n")
