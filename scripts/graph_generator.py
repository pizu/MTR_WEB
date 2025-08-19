#!/usr/bin/env python3
"""
graph_generator.py
==================
Generates summary (multi-hop overlay) graphs from RRDs based on YAML settings.

Key points:
- Accepts BOTH: `--settings /path/to/mtr_script_settings.yaml` (preferred) and legacy positional path.
- Defaults to repo-root ../mtr_script_settings.yaml when no path is given.
- Generates ONLY summary graphs (per-hop graphs removed).
- Logs to logs/graph_generator.log using project-wide logger setup.

Exit codes:
- 0: launcher ran successfully (individual job errors are logged but do not fail the whole step).
- 1: fatal launcher error (cannot read settings, job planning failed, executor failure, etc.).
"""

import os
import sys
import time
import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# --- Ensure scripts/modules are importable (works from systemd and shell) ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
for p in (MODULES_DIR, SCRIPTS_DIR, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

# --- Project helpers ---
from modules.utils import load_settings, setup_logger  # noqa: E402
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


def main() -> int:
    # 1) Load settings (fatal if unreadable)
    settings_path = resolve_settings_path()
    try:
        settings = load_settings(settings_path)
    except Exception as e:
        print(f"[FATAL] Failed to load settings '{settings_path}': {e}", file=sys.stderr)
        return 1

    # 2) Logger (honors logging_levels.graph_generator)
    logger = setup_logger(
        "graph_generator",
        settings.get("log_directory", "/tmp"),
        "graph_generator.log",
        settings=settings
    )
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
        jobs = plan_jobs_for_targets(settings, cfg, do_summary=do_summary, do_hops=False)
    except Exception as e:
        logger.error(f"Job planning failed: {e}")
        return 1

    if not jobs:
        logger.info("No graph jobs to run.")
        save_run_index(cfg.STATE_PATH, run_index + 1)
        return 0

    # 6) Execute jobs (process executor is safest with rrdtool)
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

    # 7) Advance cadence counter regardless of per-job errors
    try:
        save_run_index(cfg.STATE_PATH, run_index + 1)
    except Exception as e:
        logger.warning(f"Failed to save run index to {cfg.STATE_PATH}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
