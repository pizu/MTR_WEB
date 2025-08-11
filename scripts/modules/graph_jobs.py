#!/usr/bin/env python3
"""
Build the job list and clean stale PNGs.
Each job is a tuple: ("summary"|"hop", args_tuple)
"""
import os
from modules.graph_utils import get_labels

def _clean_old_graphs(graph_dir: str, ip: str, expected_pngs: set, logger):
    try:
        for fname in os.listdir(graph_dir):
            if fname.startswith(f"{ip}_") and fname.endswith(".png") and fname not in expected_pngs:
                try:
                    os.remove(os.path.join(graph_dir, fname))
                    logger.info(f"[CLEANED] {fname}")
                except Exception as e:
                    logger.warning(f"[SKIP CLEANUP] {fname}: {e}")
    except FileNotFoundError:
        pass

def _list_rrd_ds(rrdtool, rrd_path: str, logger):
    try:
        info = rrdtool.info(rrd_path)
        ds = set()
        for k in info.keys():
            if k.startswith("ds[") and k.endswith("].type"):
                ds.add(k[3:-6])  # 'ds[hop0_avg].type' → 'hop0_avg'
        return ds
    except Exception as e:
        logger.warning(f"RRD info failed for {rrd_path}: {e}")
        return set()

def plan_jobs_for_targets(settings, cfg, do_summary: bool, do_hops: bool):
    """
    Iterate targets, build expected PNG set, cleanup, and plan jobs.
    """
    import yaml, rrdtool  # local import to keep module import time low
    logger = settings.get("_logger") or None  # not used; logger lives in caller

    # Load targets
    try:
        with open("mtr_targets.yaml") as f:
            targets = yaml.safe_load(f).get("targets", [])
    except Exception:
        targets = []

    jobs = []
    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue

        rrd_path = os.path.join(cfg.RRD_DIR, f"{ip}.rrd")
        if not os.path.exists(rrd_path):
            continue

        hops = get_labels(ip, traceroute_dir=cfg.TRACE_DIR)
        if not hops:
            continue

        # Which PNGs should exist (used for cleanup)
        expected = set()
        for metric in cfg.DATA_SOURCES:
            for rng in cfg.TIME_RANGES:
                label = rng.get("label")
                if not label:
                    continue
                expected.add(f"{ip}_{metric}_{label}.png")
                for hop_index, _ in hops:
                    expected.add(f"{ip}_hop{hop_index}_{metric}_{label}.png")

        # Cleanup old PNGs for this IP
        _clean_old_graphs(cfg.GRAPH_DIR, ip, expected, logger=None)

        ds_present = _list_rrd_ds(rrdtool, rrd_path, logger=None)

        # Plan jobs respecting cadence flags
        for metric in cfg.DATA_SOURCES:
            for rng in cfg.TIME_RANGES:
                label = rng.get("label")
                seconds = rng.get("seconds")
                if not label or not seconds:
                    continue

                if do_summary:
                    jobs.append(("summary", (
                        ip, rrd_path, metric, label, seconds, hops,
                        cfg.WIDTH, cfg.HEIGHT, cfg.SKIP_UNCHANGED, cfg.RECENT_SAFETY_SECONDS,
                        cfg.TRACE_DIR, cfg.USE_RRD_LOCK, cfg.EXECUTOR_KIND, cfg.GRAPH_DIR,
                        cfg.CPU_AFFINITY
                    )))

                if do_hops:
                    for hop_index, hop_label in hops:
                        jobs.append(("hop", (
                            ip, rrd_path, hop_index, metric, label, seconds, ds_present, hop_label,
                            cfg.WIDTH, cfg.HEIGHT, cfg.SKIP_UNCHANGED, cfg.RECENT_SAFETY_SECONDS,
                            cfg.TRACE_DIR, cfg.USE_RRD_LOCK, cfg.EXECUTOR_KIND, cfg.GRAPH_DIR,
                            cfg.CPU_AFFINITY
                        )))
    return jobs
