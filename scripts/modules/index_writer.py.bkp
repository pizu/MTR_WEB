#!/usr/bin/env python3

# modules/index_writer.py

import os
from datetime import datetime
from modules.rrd_metrics import get_rrd_metrics
from modules.fping_status import get_fping_status

def generate_index_page(targets, settings, logger):
    """
    Builds the index.html page from the list of targets and settings.
    """
    HTML_DIR = "html"
    LOG_DIR = settings.get("log_directory", "logs")
    RRD_DIR = settings.get("rrd_directory", "rrd")
    ENABLE_FPING = settings.get("enable_fping_check", True)
    FPING_PATH = settings.get("fping_path", None)
    REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)
    DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]

    index_path = os.path.join(HTML_DIR, "index.html")
    os.makedirs(HTML_DIR, exist_ok=True)

    try:
        with open(index_path, "w") as f:
            # Header
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
<tr><th>IP</th><th>Description</th><th>Status</th><th>Last Seen</th><th>Hop0</th><th>Total</th></tr>
""")

            for target in targets:
                ip = target["ip"]
                desc = target.get("description", "")
                log_path = os.path.join(LOG_DIR, f"{ip}.log")

                # Determine last seen
                last_seen = "Never"
                if os.path.exists(log_path):
                    with open(log_path) as lf:
                        lines = [l for l in lf if "MTR RUN" in l]
                        if lines:
                            last_seen = lines[-1].split("]")[0].strip("[")

                # Determine reachability
                status = get_fping_status(ip, FPING_PATH) if ENABLE_FPING else "Unknown"

                # RRD metrics
                hop0, avg = get_rrd_metrics(ip, RRD_DIR, DATA_SOURCES)

                def fmt(metrics):
                    return ", ".join(f"{k}: {v}" for k, v in metrics.items()) if metrics else "-"

                f.write(f"<tr><td><a href='{ip}.html'>{ip}</a></td><td>{desc}</td><td>{status}</td><td>{last_seen}</td>")
                f.write(f"<td>{fmt(hop0)}</td><td>{fmt(avg)}</td></tr>\n")

            f.write(f"""</table>
<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — Auto-refresh: {'enabled' if REFRESH_SECONDS > 0 else 'disabled'}</p>
</body></html>
""")
            logger.info(f"Generated index.html with {len(targets)} targets")
    except Exception as e:
        logger.error(f"Failed to generate index.html: {e}")
