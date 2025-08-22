#!/usr/bin/env python3
"""
html_generator.py
=================
Builds per-target HTML pages using the data/graphs produced elsewhere.

Improvements:
- Accepts BOTH `--settings <path>` and legacy positional path.
- Defaults to repo-root ../mtr_script_settings.yaml if not given.
- Uses project utils to resolve HTML dir and targets path.
- Robust logging and clear failures.
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

from modules.utils import load_settings, setup_logger, resolve_html_dir, resolve_targets_path  # noqa: E402
from modules.graph_utils import get_available_hops  # noqa: E402
from modules.html_builder.target_html import generate_target_html  # noqa: E402
from modules.html_cleanup import remove_orphan_html_files  # noqa: E402


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

    logger = setup_logger(
        "html_generator",
        settings.get("log_directory", "/tmp"),
        "html_generator.log",
        settings=settings
    )

    HTML_DIR = resolve_html_dir(settings)

    # 2) Load targets
    targets_file = resolve_targets_path()
    try:
        with open(targets_file, "r", encoding="utf-8") as f:
            targets = yaml.safe_load(f).get("targets", []) or []
        logger.info(f"Loaded {len(targets)} targets from {targets_file}")
    except Exception:
        logger.exception(f"Failed to load {targets_file}")
        return 1

    # 3) Generate HTML per target
    target_ips = []
    for t in targets:
        ip = t.get("ip")
        if not ip:
            continue
        target_ips.append(ip)
        description = t.get("description", "")

        # Prefer traceroute-based labels; get_available_hops handles fallbacks.
        hops = get_available_hops(
            ip,
            graph_dir=os.path.join(HTML_DIR, "graphs"),
            traceroute_dir=settings.get("traceroute_directory", "traceroute"),
        )
        try:
            generate_target_html(ip, description, hops, settings, logger)
        except Exception:
            logger.exception(f"Failed generating HTML for {ip}")

    # 4) Cleanup orphan pages
    try:
        remove_orphan_html_files(HTML_DIR, target_ips, logger)
    except Exception:
        logger.exception("HTML cleanup failed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
