#!/usr/bin/env python3
"""
modules/index_writer.py
=======================

Small orchestrator called by index_generator.py:
- Builds the card view model using helpers
- Writes a unified index.html (sidebar + cards + theme toggle + in-page Settings drawer)
- All settings from mtr_script_settings.yaml; targets from mtr_targets.yaml

Logging
-------
Uses the logger provided by index_generator. Emits INFO about major steps,
DEBUG about ranges and counts, and WARN/ERROR on recoverable failures.
"""

from typing import Dict, Any, List
from modules.utils import resolve_html_dir, resolve_all_paths, get_html_ranges
from modules.index_helpers import build_cards
from modules.index_html_writer import write_index_html


def generate_index_page(targets: List[Dict[str, Any]], settings: Dict[str, Any], logger) -> None:
    """Entry called by index_generator.py"""
    logger.info("[index] Generating dashboard…")

    html_dir = resolve_html_dir(settings)
    paths    = resolve_all_paths(settings)

    logger.debug(f"[index] HTML_DIR={html_dir}")
    logger.debug(f"[index] PATHS={paths}")

    enable_fping = settings.get("index_page", {}).get(
        "enable_fping_check",
        settings.get("enable_fping_check", True)
    )
    auto_refresh_seconds = settings.get("html", {}).get(
        "auto_refresh_seconds",
        settings.get("html_auto_refresh_seconds", 0)
    )
    logger.debug(f"[index] enable_fping={enable_fping}, auto_refresh_seconds={auto_refresh_seconds}")

    # Use configured ranges from YAML (no hardcoding)
    ranges_cfg   = get_html_ranges(settings) or []
    range_labels = [r.get("label") for r in ranges_cfg if r.get("label")]
    if not range_labels:
        logger.warning("[index] No ranges found in settings (html.ranges). Falling back to ['15m'].")
        range_labels = ["15m"]
    default_range_label = range_labels[0]
    logger.debug(f"[index] range_labels={range_labels}, default_range_label={default_range_label}")

    # Build cards
    cards = build_cards(
        targets=targets,
        paths=paths,
        enable_fping=enable_fping,
        logger=logger
    )
    logger.info(f"[index] Prepared {len(cards)} target cards.")

    # File paths for the YAML editor drawer
    settings_path = settings.get("_loaded_from") or "mtr_script_settings.yaml"
    targets_path  = paths.get("targets", "mtr_targets.yaml")
    logger.debug(f"[index] settings_path={settings_path}, targets_path={targets_path}")

    # Write HTML
    write_index_html(
        html_dir=html_dir,
        cards=cards,
        range_labels=range_labels,
        default_range_label=default_range_label,
        auto_refresh_seconds=int(auto_refresh_seconds or 0),
        settings_path=settings_path,
        targets_path=targets_path,
        logger=logger
    )

    logger.info("[index] Dashboard generated successfully.")
