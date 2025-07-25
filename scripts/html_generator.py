#!/usr/bin/env python3
import os
import yaml
from datetime import datetime

# Load settings
with open("mtr_script_settings.yaml") as f:
    settings = yaml.safe_load(f)

LOG_DIR = settings.get("log_directory", "logs")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
HTML_DIR = "html"
TRACEROUTE_DIR = "traceroute"
LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)

# Load targets
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

# Template function
def generate_html(ip, description):
    log_path = os.path.join(LOG_DIR, f"{ip}.log")
    trace_path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    html_path = os.path.join(HTML_DIR, f"{ip}.html")

    # Load logs (latest first)
    logs = []
    if os.path.exists(log_path):
        with open(log_path) as f:
            logs = f.readlines()
        logs = [line.strip() for line in logs if line.strip()]
        logs = logs[-LOG_LINES_DISPLAY:][::-1]

    # Load traceroute
    traceroute = []
    if os.path.exists(trace_path):
        with open(trace_path) as f:
            traceroute = f.read().splitlines()

    # Build HTML
    with open(html_path, "w") as f:
        f.write(f"<html><head><title>{ip}</title><meta charset='utf-8'></head><body>")
        f.write(f"<h2>{ip}</h2>")
        if description:
            f.write(f"<p><b>{description}</b></p>")

        if traceroute:
            f.write("<h3>Traceroute</h3><pre>")
            for hop in traceroute:
                f.write(f"{hop}\n")
            f.write("</pre>")

        f.write("<h3>Graphs</h3>")
        for metric in ["avg", "last", "best", "loss"]:
            graph_file = os.path.join(GRAPH_DIR, f"{ip}_{metric}.png")
            if os.path.exists(graph_file):
                f.write(f"<div><b>{metric.upper()}:</b><br>")
                f.write(f"<img src='graphs/{ip}_{metric}.png'><br><br></div>")

        f.write("<h3>Recent Logs</h3><pre>")
        if logs:
            for line in logs:
                f.write(f"{line}\n")
        else:
            f.write("No logs available.\n")
        f.write("</pre>")

        f.write(f"<hr><p><a href='index.html'>Back to index</a></p>")
        f.write("</body></html>")

    print(f"[UPDATED] {html_path}")

# Generate per-target pages
for target in targets:
    generate_html(target["ip"], target.get("description", ""))
