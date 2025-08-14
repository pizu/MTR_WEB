#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # allow local imports from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "modules"))  # allow scripts/modules

import yaml
from modules.utils import load_settings, setup_logger, resolve_targets_path
from modules.index_writer import generate_index_page

def _default_settings_path() -> str:
    """Return the repo-root mtr_script_settings.yaml (../ from scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))

#def _targets_path() -> str:
#    """Return the repo-root mtr_targets.yaml (../ from scripts/)."""
#    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_targets.yaml"))

# Load settings (accept argv[1] from controller; fallback to repo root)
settings_path = sys.argv[1] if len(sys.argv) > 1 else _default_settings_path()
settings = load_settings(settings_path)
logger = setup_logger("index_generator", settings.get("log_directory", "/tmp"),
                      "index_generator.log", settings=settings)

# Load targets from repo root
targets_file = resolve_targets_path()
try:
    with open(targets_file, "r", encoding="utf-8") as f:
        targets = yaml.safe_load(f).get("targets", []) or []
    logger.info(f"Loaded {len(targets)} targets from {targets_file}")
except Exception as e:
    logger.error(f"Failed to load targets from {targets_file}: {e}")
    targets = []

# Generate the index.html page
generate_index_page(targets, settings, logger)
