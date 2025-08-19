#!/usr/bin/env python3
"""
graph_generator.py

Entrypoint that wires:
  settings → graph_config → job planning → parallel workers

This build generates ONLY summary graphs (multi-hop overlays)
and writes them into per-IP subfolders under cfg.GRAPH_DIR.

Per-hop graph generation has been removed.
"""

import os
import time
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from modules.utils import load_settings, setup_logger
from modules.graph_config import load_graph_config
from modules.graph_state import load_run_index, save_run_index
from modules.graph_jobs import plan_jobs_for_targets
from modules.graph_workers import graph_summary_work  # ← only summary

def main():
    # Resolve settings path (default: ../mtr_script_settings.yaml)
    default_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))
    settings_path = sys.argv[1] if len(sys.argv) > 1 else default_path

    # Load settings + logger
    settings = load_settings(settings_path)
    logger = setup_logger("graph_generator",
                          settings.get("log_directory", "/tmp"),
                          "graph_generator.log",
                          settings=settings)

    # Parse graph config, ensure output root exists
    cfg = load_graph_config(settings)
    os.makedirs(cfg.GRAPH_DIR, exist_ok=True)

    # Cadence control (run_index is persisted in a tiny JSON file)
    run_index = load_run_index(cfg.STATE_PATH)
    do_summary = (run_index % max(1, cfg.SUMMARY_EVERY) == 0)
    logger.info(f"Run #{run_index} — summaries: {'yes' if do_summary else 'no'}")

    # Build jobs (plan only returns "summary" jobs in this build)
    jobs = plan_jobs_for_targets(settings, cfg, do_summary=do_summary, do_hops=False)
    logger.info(
        f"Planned {len(jobs)} jobs "
        f"(executor={cfg.EXECUTOR_KIND}, parallelism={cfg.PARALLELISM}, skip_unchanged={cfg.SKIP_UNCHANGED})"
    )

    # Pick executor type
    Executor = ProcessPoolExecutor if cfg.EXECUTOR_KIND == "process" else ThreadPoolExecutor

    total = skipped = errors = 0
    t0 = time.time()
    with Executor(max_workers=cfg.PARALLELISM) as pool:
        futures = [pool.submit(graph_summary_work, args) for kind, args in jobs]  # all summary

        for fut in as_completed(futures):
            status, _, _ = fut.result()
            total += 1
            if status == "skipped":
                skipped += 1
            elif status == "error":
                errors += 1

    logger.info(f"Graph gen finished: jobs={total}, skipped={skipped}, errors={errors}, wall={time.time()-t0:.2f}s")

    # Bump cadence counter
    save_run_index(cfg.STATE_PATH, run_index + 1)

if __name__ == "__main__":
    main()
