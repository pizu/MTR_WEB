#!/usr/bin/env python3

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))  # allow local imports from scripts/

import yaml
from modules.utils import load_settings, setup_logger
from modules.graph_utils import get_available_hops
from modules.html_builder.target_html import generate_target_html
from modules.html_cleanup import remove_orphan_html_files, resolve_html_dir_from_scripts

def _default_settings_path() -> str:
    """Return the repo-root mtr_script_settings.yaml (../ from scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))

def resolve_html_dir_from_scripts(settings):
    html_dir = settings.get("html_directory", "html")
    if not os.path.isabs(html_dir):
        html_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", html_dir))
    else:
        html_dir = os.path.abspath(html_dir)
    os.makedirs(html_dir, exist_ok=True)
    return html_dir
def _targets_path() -> str:
    """Return the repo-root mtr_targets.yaml (../ from scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_targets.yaml"))

# Load settings and logger (accept argv[1] from controller; fallback to repo root)
settings_path = sys.argv[1] if len(sys.argv) > 1 else _default_settings_path()
settings = load_settings(settings_path)
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("html_generator", log_directory, "html_generator.log", settings=settings)

HTML_DIR = resolve_html_dir_from_scripts(settings)

# Load targets (from repo root)
targets_file = _targets_path()
try:
    with open(targets_file, "r", encoding="utf-8") as f:
        targets = yaml.safe_load(f).get("targets", []) or []
    target_ips = [t["ip"] for t in targets if t.get("ip")]
    logger.info(f"Loaded {len(targets)} targets from {targets_file}")
except Exception as e:
    logger.exception(f"Failed to load {targets_file}")
    sys.exit(1)

# Generate HTML for each target
for target in targets:
    ip = target.get("ip")
    if not ip:
        continue
    description = target.get("description", "")
    hops = get_available_hops(ip)
    generate_target_html(ip, description, hops, settings)

# Clean old files
remove_orphan_html_files(HTML_DIR, target_ips, logger)
remove_orphan_html_files(HTML_DIR, valid_ips, logger)
