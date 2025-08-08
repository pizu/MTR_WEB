#!/usr/bin/env python3
import sys
sys.path.insert(0, os.path.dirname(__file__))  # Allow local imports from scripts/modules

import yaml
from modules.utils import load_settings, setup_logger
from modules.index_writer import generate_index_page

# Load settings
settings = load_settings()
logger = setup_logger("index_generator", settings.get("log_directory", "/tmp"), "index_generator.log", settings=settings)

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
