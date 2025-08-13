#!/usr/bin/env python3
# modules/index_writer.py
#
# This version removes the Hop0 and Total columns from the index page.
# It keeps the columns: IP, Description, Status, Last Seen.
#
# It also avoids calling get_rrd_metrics() for the index for speed/clarity.

import os
from datetime import datetime
from modules.fping_status import get_fping_status

def generate_index_page(targets, settings, logger):
    """
    Builds the index.html page from the list of targets and settings.
    Columns: IP | Description | Status | Last Seen
    """
    HTML_DIR        = "html"
    LOG_DIR         = settings.get("log_directory", "logs")
    ENABLE_FPING    = settings.get("enable_fping_check", True)
    FPING_PATH      = settings.get("fping_path", None)
    REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)

    index_path = os.path.join(HTML_DIR, "index.html")
    os.makedirs(HTML_DIR, exist_ok=True)

    try:
        with open(index_path, "w", encoding="utf-8") as f:
            # Header + optional auto-refresh
            f.write("<html><head><meta charset='utf-8'>")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>")
                logger.info(f"Auto-refresh enabled: {REFRESH_SECONDS}s")
            else:
                logger.info("Auto-refresh disabled")
            f.write("""
<title>MTR Dashboard</title>
<style>
    body { font-family: Arial; margin: 20px; background: #f8f9fa; }
    table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th, td { border: 1px solid #ccc; padding: 10px 12px; text-align: left; }
    th { background-color: #f0f0f0; cursor: pointer; }
    tr:hover { background-color: #f5f5f5; }
</style>
</head><body>
<h2>MTR Monitoring Dashboard</h2>
<table>
<tr><th>IP</th><th>Description</th><th>Status</th><th>Last Seen</th></tr>
""")

            for target in targets:
                ip   = target["ip"]
                desc = target.get("description", "")
                log_path = os.path.join(LOG_DIR, f"{ip}.log")

                # Determine "Last Seen" by scanning logs for the last "MTR RUN" line
                last_seen = "Never"
                if os.path.exists(log_path):
                    try:
                        with open(log_path, encoding="utf-8") as lf:
                            lines = [l for l in lf if "MTR RUN" in l]
                            if lines:
                                # Expect format like: "2025-08-01 12:34:56,789 [INFO] MTR RUN ..."
                                last_seen = lines[-1].split("]")[0].strip("[")
                    except Exception as e:
                        logger.warning(f"Failed reading last_seen from {log_path}: {e}")

                # Determine reachability status (optional fping check)
                status = get_fping_status(ip, FPING_PATH) if ENABLE_FPING else "Unknown"

                # Row with link to the target page
                f.write(
                    f"<tr>"
                    f"<td><a href='{ip}.html'>{ip}</a></td>"
                    f"<td>{desc}</td>"
                    f"<td>{status}</td>"
                    f"<td>{last_seen}</td>"
                    f"</tr>\n"
                )

            f.write(f"""</table>
<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — Auto-refresh: {'enabled' if REFRESH_SECONDS > 0 else 'disabled'}</p>
</body></html>
""")
            logger.info(f"Generated index.html with {len(targets)} targets")
    except Exception as e:
        logger.error(f"Failed to generate index.html: {e}")
