#!/usr/bin/env python3
# modules/html_cleanup.py

import os
import sys

HTML_DIR = resolve_html_dir_from_scripts(settings)

def resolve_html_dir_from_scripts(settings):
    html_dir = settings.get("html_directory", "html")
    if not os.path.isabs(html_dir):
        html_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", html_dir))
    else:
        html_dir = os.path.abspath(html_dir)
    os.makedirs(html_dir, exist_ok=True)
    return html_dir

def remove_orphan_html_files(HTML_DIR, valid_ips, logger):
    """
    Removes any *.html files (except index.html) that do not correspond to current IPs.

    Args:
        html_dir (str): Path to the HTML output directory.
        valid_ips (list): List of valid IPs from mtr_targets.yaml.
        logger (logging.Logger): Logger instance to use for logging.
    """
    try:
        all_html = [f for f in os.listdir(html_dir) if f.endswith(".html") and f != "index.html"]
        # remove any per-hop landing pages
        for html_file in list(all_html):
            if html_file.endswith("_hops.html"):
                os.remove(os.path.join(html_dir, html_file))
                logger.info(f"Removed per-hop HTML: {html_file}")
        # remove pages for IPs no longer present
        for html_file in all_html:
            ip_clean = html_file.replace(".html", "")
            if ip_clean not in valid_ips:
                os.remove(os.path.join(html_dir, html_file))
                logger.info(f"Removed stale HTML file: {html_file}")

        # also purge old per-hop PNGs
        graphs_dir = os.path.join(html_dir, "graphs")
        if os.path.isdir(graphs_dir):
            for f in os.listdir(graphs_dir):
                if "_hop" in f and f.endswith(".png"):
                    os.remove(os.path.join(graphs_dir, f))
                    logger.info(f"Removed per-hop PNG: {f}")
    except Exception as e:
        logger.warning(f"Failed to clean orphan HTML/graph files: {e}")
