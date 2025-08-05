#!/usr/bin/env python3
import os
import yaml
from utils import load_settings, setup_logger
from html_builder import generate_target_html
from graph_utils import get_available_hops

# Load settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("html_generator", settings.get("log_directory", "/tmp"), "html_generator.log", settings=settings)

HTML_DIR = "html"

# Load targets
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f)["targets"]
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
    generate_target_html(ip, description, hops, settings, logger)

# Clean old files
try:
    all_html = [f for f in os.listdir(HTML_DIR) if f.endswith(".html") and f != "index.html"]
    for f in all_html:
        ip_clean = f.replace("_hops.html", "").replace(".html", "")
        if ip_clean not in target_ips:
            os.remove(os.path.join(HTML_DIR, f))
            logger.info(f"Removed stale HTML file: {f}")
except Exception as e:
    logger.warning(f"Failed to clean orphan HTML: {e}")
