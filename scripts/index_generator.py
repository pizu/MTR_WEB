#!/usr/bin/env python3
"""
index_generator.py
==================
Builds the top-level index.html (links to per-target pages).

Improvements:
- Accepts BOTH `--settings <path>` and legacy positional path.
- Defaults to repo-root ../mtr_script_settings.yaml if not given.
- Uses project utils to locate targets file and set up logging.
"""

import os
import sys
import argparse
import yaml

# Ensure imports work under systemd
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")
for p in (MODULES_DIR, SCRIPTS_DIR, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from modules.utils import load_settings, setup_logger, resolve_targets_path  # noqa: E402
from modules.index_writer import generate_index_page  # noqa: E402


def resolve_settings_path(default_name: str = "mtr_script_settings.yaml") -> str:
    """--settings <path> → positional → ../mtr_script_settings.yaml"""
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
    # 1) Settings + logger
    settings_path = resolve_settings_path()
    try:
        settings = load_settings(settings_path)
    except Exception as e:
        print(f"[FATAL] Failed to load settings '{settings_path}': {e}", file=sys.stderr)
        return 1

    logger = setup_logger("index_generator", settings=settings)

    # 2) Load targets
    targets_file = resolve_targets_path()
    try:
        with open(targets_file, "r", encoding="utf-8") as f:
            targets = yaml.safe_load(f).get("targets", []) or []
        logger.info(f"Loaded {len(targets)} targets from {targets_file}")
    except Exception:
        logger.exception(f"Failed to load targets from {targets_file}")
        return 1

    # 3) Build index.html
    try:
        generate_index_page(targets, settings, logger)
    except Exception:
        logger.exception("Failed to generate index.html")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
