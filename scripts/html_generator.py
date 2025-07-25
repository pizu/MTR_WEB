#!/usr/bin/env python3
import os
import yaml
from datetime import datetime
from utils import load_settings, setup_logger

# Load settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("html_generator", log_directory, "html_generator.log")

# Directories
LOG_DIR = settings.get("log_directory", "logs")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
HTML_DIR = "html"
TRACEROUTE_DIR = "traceroute"
LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)

# Load targets
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f)["targets"]
    target_ips = [t["ip"] for t in targets]
    logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
except Exception as e:
    logger.exception("Failed to load mtr_targets.yaml")
    exit(1)

# Template function
def generate_html(ip, description):
    log_path = os.path.join(LOG_DIR, f"{ip}.log")
    trace_path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    html_path = os.path.join(HTML_DIR, f"{ip}.html")

    # Load logs
    logs = []
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                logs = f.readlines()
            logs = [line.strip() for line in logs if line.strip()]
            logs = logs[-LOG_LINES_DISPLAY:][::-1]
        except Exception as e:
            logger.warning(f"Could not read logs for {ip}: {e}")
    else:
        logger.warning(f"No log file found for {ip} at {log_path}")

    # Load traceroute
    traceroute = []
    if os.path.exists(trace_path):
        try:
            with open(trace_path) as f:
                traceroute = f.read().splitlines()
        except Exception as e:
            logger.warning(f"Could not read traceroute for {ip}: {e}")
    else:
        logger.warning(f"No traceroute file found for {ip} at {trace_path}")

    # Build HTML
    try:
        with open(html_path, "w") as f:
            f.write("<html><head>")
            f.write(f"<title>{ip}</title><meta charset='utf-8'>")
            f.write("</head><body>")

            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")

            f.write(f"<p><i>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i></p>")

            # Traceroute table
            if traceroute:
                f.write("<h3>Traceroute</h3>")
                f.write("<table border='1' cellspacing='0' cellpadding='5'>")
                f.write("<tr><th>Hop</th><th>Address / Hostname</th><th>Details</th></tr>")
                for line in traceroute:
                    parts = line.strip().split()
                    if not parts:
                        continue
                    hop = parts[0]
                    address = parts[1] if len(parts) > 1 else "-"
                    detail = " ".join(parts[2:]) if len(parts) > 2 else "-"
                    f.write(f"<tr><td>{hop}</td><td>{address}</td><td>{detail}</td></tr>")
                f.write("</table>")

                # Link to raw trace file
                f.write(f"<p><a href='../{TRACEROUTE_DIR}/{ip}.trace.txt' target='_blank'>Download traceroute text</a></p>")
            else:
                f.write("<p><i>No traceroute data available.</i></p>")

            # Graphs
            f.write("<h3>Graphs (Last 24h)</h3>")
            for metric in ["avg", "last", "best", "loss"]:
                graph_file = os.path.join(GRAPH_DIR, f"{ip}_{metric}.png")
                if os.path.exists(graph_file):
                    f.write(f"<div><b>{metric.upper()}:</b><br>")
                    f.write(f"<img src='graphs/{ip}_{metric}.png'><br><br></div>")
                else:
                    logger.debug(f"Graph not found: {graph_file}")

            # Logs
            f.write("<h3>Recent Logs</h3><pre>")
            if logs:
                for line in logs:
                    f.write(f"{line}\n")
            else:
                f.write("No logs available.\n")
            f.write("</pre>")

            f.write("<hr><p><a href='index.html'>Back to index</a></p>")
            f.write("</body></html>")
        logger.info(f"Updated HTML page for {ip}")
    except Exception as e:
        logger.exception(f"Failed to write HTML for {ip}: {e}")

# Generate HTML per target
for target in targets:
    ip = target["ip"]
    desc = target.get("description", "")
    generate_html(ip, desc)

# Cleanup orphan .html files
try:
    all_html_files = [f for f in os.listdir(HTML_DIR) if f.endswith(".html") and f != "index.html"]
    for f in all_html_files:
        ip_from_file = f.replace(".html", "")
        if ip_from_file not in target_ips:
            path_to_delete = os.path.join(HTML_DIR, f)
            os.remove(path_to_delete)
            logger.info(f"Removed stale HTML file: {path_to_delete}")
except Exception as e:
    logger.warning(f"Failed to clean orphan HTML files: {e}")
