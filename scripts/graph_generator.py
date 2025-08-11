#!/usr/bin/env python3
"""
Entrypoint that wires config → jobs → executor → workers.
Keeps the main file tiny and readable.
"""
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from modules.utils import load_settings, setup_logger
from modules.graph_config import load_graph_config
from modules.graph_state import load_run_index, save_run_index
from modules.graph_jobs import plan_jobs_for_targets
from modules.graph_workers import graph_summary_work, graph_hop_work

def main():
    settings = load_settings("mtr_script_settings.yaml")
    logger = setup_logger("graph_generator", settings.get("log_directory", "/tmp"),
                          "graph_generator.log", settings=settings)

    cfg = load_graph_config(settings)
    os.makedirs(cfg.GRAPH_DIR, exist_ok=True)

    # cadence: which jobs to run this round
    run_index = load_run_index(cfg.STATE_PATH)
    do_summary = (run_index % max(1, cfg.SUMMARY_EVERY) == 0)
    do_hops    = (run_index % max(1, cfg.HOPS_EVERY) == 0)
    logger.info(f"Run #{run_index} — summaries: {'yes' if do_summary else 'no'}, hops: {'yes' if do_hops else 'no'}")

    jobs = plan_jobs_for_targets(settings, cfg, do_summary=do_summary, do_hops=do_hops)
    logger.info(f"Planned {len(jobs)} jobs (executor={cfg.EXECUTOR_KIND}, parallelism={cfg.PARALLELISM}, skip_unchanged={cfg.SKIP_UNCHANGED})")

    Executor = ProcessPoolExecutor if cfg.EXECUTOR_KIND == "process" else ThreadPoolExecutor

    total = skipped = errors = 0
    t0 = time.time()
    with Executor(max_workers=cfg.PARALLELISM) as pool:
        futures = []
        for kind, args in jobs:
            if kind == "summary":
                futures.append(pool.submit(graph_summary_work, args))
            else:
                futures.append(pool.submit(graph_hop_work, args))

        for fut in as_completed(futures):
            status, _, _ = fut.result()
            total += 1
            if status == "skipped":
                skipped += 1
            elif status == "error":
                errors += 1

    logger.info(f"Graph gen finished: jobs={total}, skipped={skipped}, errors={errors}, wall={time.time()-t0:.2f}s")

    # bump cadence counter
    save_run_index(cfg.STATE_PATH, run_index + 1)

if __name__ == "__main__":
    main()
