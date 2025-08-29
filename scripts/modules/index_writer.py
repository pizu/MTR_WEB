#!/usr/bin/env python3
"""
modules/index_writer.py
=======================

Tiny orchestrator used by index_generator.py:
- Builds the card model (per target) using helpers
- Writes a unified index.html (sidebar + cards + theme toggle + in-page Settings drawer)
- (Optional) Also writes a standalone settings.html if you ever want it (disabled by default)

All settings are sourced from mtr_script_settings.yaml (ranges, paths, refresh, etc.)
Targets come from mtr_targets.yaml (loaded by index_generator.py).

"""

from typing import Dict, Any, List
from modules.utils import resolve_html_dir, resolve_all_paths, get_html_ranges
from modules.index_helpers import build_cards
from modules.index_html_writer import write_index_html


def generate_index_page(targets: List[Dict[str, Any]], settings: Dict[str, Any], logger) -> None:
    """
    Required by index_generator.py. Creates the top-level Dashboard page.

    Parameters
    ----------
    targets  : list[dict]
        From mtr_targets.yaml (index_generator loads it).
    settings : dict
        From mtr_script_settings.yaml (loaded via modules.utils.load_settings).
    logger   : logging.Logger
        Standard project logger.
    """
    html_dir = resolve_html_dir(settings)
    paths    = resolve_all_paths(settings)

    # Whether to use fping for quick status on the index
    enable_fping = settings.get("index_page", {}).get(
        "enable_fping_check",
        settings.get("enable_fping_check", True)
    )

    # Refresh meta tag for the index
    auto_refresh_seconds = settings.get("html", {}).get(
        "auto_refresh_seconds",
        settings.get("html_auto_refresh_seconds", 0)
    )

    # Pull *configured* ranges (labels) from YAML (no hard-coding)
    ranges_cfg   = get_html_ranges(settings) or []
    range_labels = [r.get("label") for r in ranges_cfg if r.get("label")] or ["15m"]
    default_range_label = range_labels[0]

    # Build card view-model for all targets (status, last seen, hop count, etc.)
    cards = build_cards(
        targets=targets,
        paths=paths,
        enable_fping=enable_fping,
        logger=logger
    )

    # Embed the live YAML text in the page so Settings drawer can show/edit it
    settings_path = settings.get("_loaded_from") or "mtr_script_settings.yaml"
    targets_path  = paths.get("targets", "mtr_targets.yaml")

    # Write the unified Dashboard (includes Settings drawer)
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
