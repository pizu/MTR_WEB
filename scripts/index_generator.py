#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # Allow local imports from scripts/modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "modules"))  # allow scripts/modules

import yaml
import subprocess
from modules.utils import load_settings, setup_logger
from modules.index_writer import generate_index_page

# Load settings
return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))
settings_path = sys.argv[1] if len(sys.argv) > 1 else _default_settings_path()
settings = load_settings(settings_path)

# Load targets from mtr_targets.yaml
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f).get("targets", [])
    logger.info(f"Loaded {len(targets)} targets")
except Exception as e:
    logger.error(f"Failed to load targets: {e}")
    targets = []

# Generate the index.html page
generate_index_page(targets, settings, logger)
