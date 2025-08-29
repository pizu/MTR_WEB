#!/usr/bin/env python3
# modules/html_cleanup.py

import os

def remove_orphan_html_files(html_dir, valid_ips, logger):
    """
    Removes any *.html files (except index.html) that do not correspond to current IPs.
    (PNG cleanup removed; project no longer produces PNG graphs.)
    """
    try:
        all_html = [f for f in os.listdir(html_dir) if f.endswith(".html") and f != "index.html"]

        # remove any old per-hop landing pages
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

    except Exception as e:
        logger.warning(f"Failed to clean orphan HTML files: {e}")
