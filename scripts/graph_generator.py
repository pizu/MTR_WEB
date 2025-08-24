#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
graph_generator.py
==================
Generates summary (multi-hop overlay) graphs from RRDs based on YAML settings.

What this launcher does
-----------------------
1) Finds and loads your settings YAML.
2) Initializes project-wide logging.
3) Loads graph configuration (dimensions, executor kind, paths, cadence).
4) Plans summary-graph jobs (one per ip × label × metric).
5) **Derives the metric list from the RRD schema in your YAML** and
   filters out any job whose metric is not present in the schema.
   This prevents mass failures when the job planner or legacy configs
   propose metrics your RRDs do not contain.
6) Runs the jobs in parallel (process or threads).
7) Reports a concise summary (total/skipped/errors) and advances cadence.

Why this exists
---------------
Your RRDs expose DS names like: hop{N}_<metric>. The list of <metric> names
should be defined in YAML under settings['rrd']['data_sources'] (each entry's
'name' key). When job planning uses a different metric set, the worker would
fail all jobs (one per metric), leading to the "errors=468" pattern you saw.

This launcher enforces that only schema-approved metrics are queued.

Exit codes
----------
- 0: launcher ran successfully (per-job failures do not fail the whole step)
- 1: fatal launcher error (settings, config, executor failure, etc.)
"""

from __future__ import annotations

import os
import sys
import time
import argparse
from typing import Any, Dict, Iterable, List, Sequence, Tuple

# --- Ensure scripts/modules are importable (works from systemd and shell) ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
for _p in (MODULES_DIR, SCRIPTS_DIR, REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# --- Project helpers ---
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from modules.utils import load_settings, setup_logger, resolve_all_paths  # noqa: E402
from modules.graph_config import load_graph_config     # noqa: E402
from modules.graph_state import load_run_index, save_run_index  # noqa: E402
from modules.graph_jobs import plan_jobs_for_targets   # noqa: E402
from modules.graph_workers import graph_summary_work   # noqa: E402  # summary-only worker


# ----------------------------
# Settings path resolver
# ----------------------------
def resolve_settings_path(default_name: str = "mtr_script_settings.yaml") -> str:
    """
    Resolve the YAML path in a backward-compatible way.

    Priority:
      1) --settings <path>
      2) first positional arg (legacy)
      3) ../mtr_script_settings.yaml (repo root)
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--settings", dest="settings", default=None)
    known, _ = parser.parse_known_args()
    if known.settings and known.settings != "--settings":
        return os.path.abspath(known.settings)
    for tok in sys.argv[1:]:
        if not tok.startswith("-"):
            return os.path.abspath(tok)
    return os.path.abspath(os.path.join(REPO_ROOT, default_name))


# ----------------------------
# Metric resolution
# ----------------------------
def _derive_schema_metrics(settings: Dict[str, Any]) -> List[str]:
    """
    Extract the list of metric *suffixes* from the YAML schema so we only
    graph what your RRDs actually contain (RRD DS names look like hopX_<metric>).

    Supports:
      rrd:
        data_sources:
          - { name: avg,   ... }
          - { name: best,  ... }
          - { name: worst, ... }
          - { name: loss,  ... }
    Also accepts a list of strings: ["avg","best","worst","loss"].

    Returns a de-duplicated list, preserving YAML order. Falls back to the
    common set if YAML is missing.
    """
    metrics: List[str] = []
    ds_list = (settings.get("rrd", {}) or {}).get("data_sources") or []
    if isinstance(ds_list, (list, tuple)):
        for item in ds_list:
            if isinstance(item, dict) and item.get("name"):
                name = str(item["name"]).strip()
                if name and name not in metrics:
                    metrics.append(name)
            elif isinstance(item, str):
                name = item.strip()
                if name and name not in metrics:
                    metrics.append(name)
    if not metrics:
        metrics = ["avg", "best", "worst", "loss"]
    return metrics


def _job_metric(job: Tuple[str, Sequence[Any]]) -> str:
    """
    Our summary worker expects args as:
      (ip, rrd_path, metric, label, seconds, hops, width, height,
       skip_unchanged, recent_safety_seconds, trace_dir, use_lock, exec_kind,
       graph_dir, cpu_affinity)
    So 'metric' is args[2].
    """
    _, args = job
    try:
        return str(args[2])
    except Exception:
        return ""


def _filter_jobs_by_metrics(jobs: List[Tuple[str, Sequence[Any]]], allowed: Iterable[str]) -> List[Tuple[str, Sequence[Any]]]:
    allowed_set = set(allowed)
    return [j for j in jobs if _job_metric(j) in allowed_set]


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    # 1) Load settings (fatal if unreadable)
    settings_path = resolve_settings_path()
    try:
        settings = load_settings(settings_path)
    except Exception as e:
        print(f"[FATAL] Failed to load settings '{settings_path}': {e}", file=sys.stderr)
        return 1

    # 2) Logger (honors logging_levels.graph_generator)
    _ = resolve_all_paths(settings)  # ensure dirs exist early (logs dir for handler)
    logger = setup_logger("graph_generator", settings=settings)
    logger.debug(f"Using settings: {settings_path}")

    # 3) Build config + ensure output directory exists
    cfg = load_graph_config(settings)
    try:
        os.makedirs(cfg.GRAPH_DIR, exist_ok=True)
    except Exception as e:
        logger.error(f"Cannot create graph output dir {cfg.GRAPH_DIR}: {e}")
        return 1

    # 4) Cadence: decide if we render summaries on this run
    run_index = load_run_index(cfg.STATE_PATH)
    do_summary = (run_index % max(1, cfg.SUMMARY_EVERY) == 0)
    logger.info(
        f"Run #{run_index} — summaries: {'yes' if do_summary else 'no'} "
        f"(executor={cfg.EXECUTOR_KIND}, parallelism={cfg.PARALLELISM}, skip_unchanged={cfg.SKIP_UNCHANGED})"
    )

    # 5) Plan jobs (summary only in this build)
    try:
        all_jobs = plan_jobs_for_targets(settings, cfg, do_summary=do_summary, do_hops=False)
    except Exception as e:
        logger.error(f"Job planning failed: {e}")
        return 1

    if not all_jobs:
        logger.info("No graph jobs to run.")
        save_run_index(cfg.STATE_PATH, run_index + 1)
        return 0

    # 6) **Schema-driven metric filter** (prevents mass failures)
    schema_metrics = _derive_schema_metrics(settings)
    jobs = _filter_jobs_by_metrics(all_jobs, schema_metrics)

    if not jobs:
        logger.warning(
            "No jobs after filtering by schema metrics. "
            f"Schema metrics={schema_metrics}; planned={len(all_jobs)}; filtered=0."
        )
        save_run_index(cfg.STATE_PATH, run_index + 1)
        return 0

    # Log visibility
    logger.info(f"Schema metrics: {', '.join(schema_metrics)}")
    if len(jobs) != len(all_jobs):
        logger.info(f"Filtered {len(all_jobs) - len(jobs)} job(s) not in schema; running {len(jobs)} job(s).")

    # 7) Execute jobs (process executor is safest with rrdtool)
    Executor = ProcessPoolExecutor if cfg.EXECUTOR_KIND == "process" else ThreadPoolExecutor

    total = skipped = errors = 0
    t0 = time.time()
    try:
        with Executor(max_workers=cfg.PARALLELISM) as pool:
            futures = [pool.submit(graph_summary_work, args) for kind, args in jobs]
            for fut in as_completed(futures):
                try:
                    status, _, _ = fut.result()
                except Exception as e:
                    status = "error"
                    logger.error(f"Worker crashed: {e}")

                total += 1
                if status == "skipped":
                    skipped += 1
                elif status == "error":
                    errors += 1
    except Exception as e:
        logger.error(f"Executor failure: {e}")
        return 1

    wall = time.time() - t0
    logger.info(f"Graph gen finished: jobs={total}, skipped={skipped}, errors={errors}, wall={wall:.2f}s")

    # 8) Advance cadence counter regardless of per-job errors
    try:
        save_run_index(cfg.STATE_PATH, run_index + 1)
    except Exception as e:
        logger.warning(f"Failed to save run index to {cfg.STATE_PATH}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
