#!/usr/bin/env python3
"""
graph_generator.py
==================
Generates summary (multi-hop overlay) graphs from RRDs based on YAML settings.

This build:
- **Only** generates summary graphs (per-hop graphs removed).
- Writes per-target graph PNGs into per-IP subfolders under cfg.GRAPH_DIR.
- Supports BOTH:
    • new style:  --settings /path/to/mtr_script_settings.yaml
    • legacy:     graph_generator.py /path/to/mtr_script_settings.yaml
  If no path is provided, defaults to the repo root's mtr_script_settings.yaml.

Exit codes:
- 0 = launcher ran successfully (even if some graph jobs reported "error"; those are logged).
- Non-zero only if the script failed to launch due to a fatal error (e.g., settings unreadable).
"""

import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# --- Make "scripts/modules" importable (works from systemd and shell) ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
if MODULES_DIR not in sys.path:
    sys.path.insert(0, MODULES_DIR)

# ----------------------------
# Settings path resolver
# ----------------------------
import argparse

def resolve_settings_path(default_name: str = "mtr_script_settings.yaml") -> str:
    """
    Resolve the settings YAML path in a backward-compatible way.

    Priority:
      1) --settings <path>      (preferred; used by controller/pipeline)
      2) first positional arg   (legacy style)
      3) repo-root default: ../mtr_script_settings.yaml

    Returns an absolute path. Does not verify existence.
    """
    # 1) Prefer explicit --settings if present
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--settings", dest="settings", default=None)
    known, _ = parser.parse_known_args()
    if known.settings and known.settings != "--settings":
        return os.path.abspath(known.settings)

    # 2) Legacy positional fallback: first non-option token
    for tok in sys.argv[1:]:
        if not tok.startswith("-"):
            return os.path.abspath(tok)

    # 3) Default to repo root file (../mtr_script_settings.yaml)
    return os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir, default_name))


# ----------------------------
# Project imports (after sys.path fix)
# ----------------------------
from modules.utils import load_settings, setup_logger
from modules.graph_config import load_graph_config
from modules.graph_state import load_run_index, save_run_index
from modules.graph_jobs import plan_jobs_for_targets
from modules.graph_workers import graph_summary_work  # summary-only worker


def main() -> int:
    """
    Orchestrates:
      settings → graph_config → job planning → parallel execution → state bump
    """
    # --- 1) Resolve and load settings (fatal if path bad or YAML unreadable) ---
    settings_path = resolve_settings_path()
    try:
        settings = load_settings(settings_path)
    except Exception as e:
        # Can't load settings → fatal for this script (return non-zero so pipeline reports failure)
        print(f"[FATAL] Failed to load settings from {settings_path}: {e}", file=sys.stderr)
        return 1

    # --- 2) Logger (respects logging_levels.graph_generator from YAML if present) ---
    logger = setup_logger(
        "graph_generator",
        settings.get("log_directory", "/tmp"),
        "graph_generator.log",
        settings=settings
    )
    logger.debug(f"Using settings: {settings_path}")

    # --- 3) Build graph config from settings and ensure output roots exist ---
    cfg = load_graph_config(settings)
    try:
        os.makedirs(cfg.GRAPH_DIR, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to ensure graph output directory {cfg.GRAPH_DIR}: {e}")
        return 1

    # --- 4) Cadence: decide whether to do summary now; increment run_index later ---
    run_index = load_run_index(cfg.STATE_PATH)
    do_summary = (run_index % max(1, cfg.SUMMARY_EVERY) == 0)
    logger.info(f"Run #{run_index} — summaries: {'yes' if do_summary else 'no'} "
                f"(executor={cfg.EXECUTOR_KIND}, parallelism={cfg.PARALLELISM}, "
                f"skip_unchanged={cfg.SKIP_UNCHANGED})")

    # --- 5) Plan jobs (this build plans only summary jobs) ---
    try:
        jobs = plan_jobs_for_targets(settings, cfg, do_summary=do_summary, do_hops=False)
    except Exception as e:
        logger.error(f"Job planning failed: {e}")
        return 1

    if not jobs:
        logger.info("No graph jobs to run (nothing changed or no targets).")
        # Still bump run index to keep cadence predictable
        save_run_index(cfg.STATE_PATH, run_index + 1)
        return 0

    # --- 6) Choose executor (process = safest with C extensions / rrdtool; thread also supported) ---
    Executor = ProcessPoolExecutor if cfg.EXECUTOR_KIND == "process" else ThreadPoolExecutor

    # --- 7) Execute jobs in parallel; collect results robustly ---
    total = skipped = errors = 0
    t0 = time.time()
    try:
        with Executor(max_workers=cfg.PARALLELISM) as pool:
            # jobs is a list of tuples like: [("summary", args), ...]
            futures = [pool.submit(graph_summary_work, args) for kind, args in jobs]

            for fut in as_completed(futures):
                try:
                    status, _, _ = fut.result()
                except Exception as e:
                    # A worker crashed before returning a structured result
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

    # --- 8) Bump cadence counter and exit (0 even if errors>0; errors are logged per policy) ---
    try:
        save_run_index(cfg.STATE_PATH, run_index + 1)
    except Exception as e:
        logger.warning(f"Failed to save run index to {cfg.STATE_PATH}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
