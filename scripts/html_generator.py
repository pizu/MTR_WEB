#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

import yaml
from modules.utils import load_settings, setup_logger, resolve_html_dir
from modules.graph_utils import get_available_hops
from modules.html_builder.target_html import generate_target_html
from modules.html_cleanup import remove_orphan_html_files

def _default_settings_path() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_script_settings.yaml"))

def _targets_path() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mtr_targets.yaml"))

# Load settings + logger
settings_path = sys.argv[1] if len(sys.argv) > 1 else _default_settings_path()
settings = load_settings(settings_path)
logger = setup_logger("html_generator", settings.get("log_directory", "/tmp"),
                      "html_generator.log", settings=settings)

HTML_DIR = resolve_html_dir(settings)

# Load targets
targets_file = _targets_path()
try:
    with open(targets_file, "r", encoding="utf-8") as f:
        targets = yaml.safe_load(f).get("targets", []) or []
    target_ips = [t["ip"] for t in targets if t.get("ip")]
    logger.info(f"Loaded {len(targets)} targets from {targets_file}")
except Exception:
    logger.exception(f"Failed to load {targets_file}")
    sys.exit(1)

# Generate HTML
for t in targets:
    ip = t.get("ip")
    if not ip:
        continue
    description = t.get("description", "")
    # Prefer traceroute labels; fallback inside get_available_hops()
    hops = get_available_hops(
        ip,
        graph_dir=os.path.join(HTML_DIR, "graphs"),
        traceroute_dir=settings.get("traceroute_directory", "traceroute")
    )
    generate_target_html(ip, description, hops, settings)

# Cleanup
remove_orphan_html_files(HTML_DIR, target_ips, logger)
