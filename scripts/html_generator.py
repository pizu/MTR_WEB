#!/usr/bin/env python3

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # Allow local imports from scripts/modules

import yaml
from modules.utils import load_settings, setup_logger
from modules.graph_utils import get_available_hops
from modules.html_builder.target_html import generate_target_html
from modules.html_builder.per_hop_html import generate_per_hop_html
from modules.html_cleanup import remove_orphan_html_files

# Load settings and logger
settings = load_settings("mtr_script_settings.yaml")
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("html_generator", log_directory, "html_generator.log", settings=settings)

HTML_DIR = "html"

# Load targets
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f).get("targets", [])
    target_ips = [t["ip"] for t in targets]
    logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
except Exception as e:
    logger.exception("Failed to load mtr_targets.yaml")
    exit(1)

# Generate HTML for each target
for target in targets:
    ip = target["ip"]
    description = target.get("description", "")
    hops = get_available_hops(ip)

    generate_target_html(ip, description, hops)
    generate_per_hop_html(ip, hops, description)

# Clean old files
remove_orphan_html_files(HTML_DIR, target_ips, logger)
