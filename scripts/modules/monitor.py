#!/usr/bin/env python3
import os
import time
from deepdiff import DeepDiff  # Library to compare previous and current hop paths

# Import modular functions
from modules.mtr_runner import run_mtr
from modules.rrd_handler import init_rrd, init_per_hop_rrds, update_rrd
from modules.trace_exporter import save_trace_and_json
from modules.severity import evaluate_severity_rules, hops_changed

def monitor_target(ip, source_ip, settings, logger):
    """
    Main loop that monitors a given IP using MTR.
    Updates RRD files and logs changes in path or loss.
    """

    # Setup paths and configs
    rrd_dir = settings.get("rrd_directory", "rrd")
    log_directory = settings.get("log_directory", "/tmp")
    interval = settings.get("interval_seconds", 60)
    severity_rules = settings.get("log_severity_rules", [])

    # Main RRD file for the full hop sequence
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    debug_rrd_log = os.path.join(log_directory, "rrd_debug.log")

    # Ensure RRDs exist
    os.makedirs(rrd_dir, exist_ok=True)
    init_rrd(rrd_path, settings, logger)
    init_per_hop_rrds(ip, settings, logger)

    # Keep track of last known state
    prev_hops = []
    prev_loss_state = {}

    logger.info(f"[{ip}] Monitoring loop started — running MTR")

    # Main loop: run MTR every X seconds
    while True:
        hops = run_mtr(ip, source_ip, logger)

        if not hops:
            logger.warning(f"[{ip}] MTR returned no data — target unreachable or command failed")
            time.sleep(interval)
            continue

        # Detect path or loss changes
        hop_path_changed = hops_changed(prev_hops, hops)
        curr_loss_state = {
            h.get("count"): round(h.get("Loss%", 0), 2)
            for h in hops if h.get("Loss%", 0) > 0
        }
        loss_changed = curr_loss_state != prev_loss_state

        # If hop path changed, log the diff
        if hop_path_changed:
            diff = DeepDiff(
                [h.get("host") for h in prev_hops],
                [h.get("host") for h in hops],
                ignore_order=False
            )
            context = {
                "hop_changed": True,
                "hop_added": bool(diff.get("iterable_item_added")),
                "hop_removed": bool(diff.get("iterable_item_removed")),
            }
            for key, value in diff.get("values_changed", {}).items():
                hop_index = key.split("[")[-1].rstrip("]")
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, level.lower(), logger.info) if tag and level else logger.info
                msg = f"[{ip}] Hop {hop_index} changed from {value.get('old_value')} to {value.get('new_value')}"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # If loss state changed, log it
        if loss_changed:
            for hop_num, loss in curr_loss_state.items():
                context = {
                    "loss": loss,
                    "prev_loss": prev_loss_state.get(hop_num, 0),
                    "hop_changed": hop_path_changed,
                }
                tag, level = evaluate_severity_rules(severity_rules, context)
                log_fn = getattr(logger, level.lower(), logger.warning if loss > 0 else logger.info) if isinstance(level, str) else (logger.warning if loss > 0 else logger.info)
                msg = f"[{ip}] Loss at hop {hop_num}: {loss}% (prev: {context['prev_loss']}%)"
                log_fn(f"[{tag}] {msg}" if tag else msg)

        # Always update RRD, even if no changes
        update_rrd(rrd_path, hops, ip, settings, debug_rrd_log)

        if hop_path_changed or loss_changed:
            logger.debug(f"[{ip}] Parsed hops: {[ (h.get('count'), h.get('host'), h.get('Avg')) for h in hops ]}")
            save_trace_and_json(ip, hops, settings, logger)
            logger.info(f"[{ip}] Traceroute and hop map saved.")
        else:
            logger.debug(f"[{ip}] No change detected — {len(hops)} hops parsed. RRD still updated.")

        # Sleep before next probe
        prev_hops = hops
        prev_loss_state = curr_loss_state
        time.sleep(interval)
