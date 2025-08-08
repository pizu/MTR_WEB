#!/usr/bin/env python3
# modules/html_cleanup.py

import os

def remove_orphan_html_files(html_dir, valid_ips, logger):
    """
    Removes any *.html files (except index.html) that do not correspond to current IPs.

    Args:
        html_dir (str): Path to the HTML output directory.
        valid_ips (list): List of valid IPs from mtr_targets.yaml.
        logger (logging.Logger): Logger instance to use for logging.
    """
    try:
        all_html = [
            f for f in os.listdir(html_dir)
            if f.endswith(".html") and f != "index.html"
        ]

        for html_file in all_html:
            ip_clean = html_file.replace("_hops.html", "").replace(".html", "")
            if ip_clean not in valid_ips:
                os.remove(os.path.join(html_dir, html_file))
                logger.info(f"Removed stale HTML file: {html_file}")
    except Exception as e:
        logger.warning(f"Failed to clean orphan HTML files: {e}")
