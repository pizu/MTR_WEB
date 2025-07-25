#!/usr/bin/env python3
import os
import re
import yaml
from datetime import datetime
from utils import load_settings, setup_logger

settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("html_generator", log_directory, "html_generator.log")

LOG_DIR = settings.get("log_directory", "logs")
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
HTML_DIR = "html"
TRACEROUTE_DIR = "traceroute"
LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)
TIME_RANGES = settings.get("graph_time_ranges", [{"label": "1h", "seconds": 3600}])
REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)

# Load targets
targets = []
target_ips = []
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f).get("targets", [])
        target_ips = [t["ip"] for t in targets]
        logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
except Exception as e:
    logger.exception("Failed to load mtr_targets.yaml")
    exit(1)

def get_available_hops(ip):
    hops = set()
    for fname in os.listdir(GRAPH_DIR):
        match = re.match(rf"{re.escape(ip)}_hop(\d+)_", fname)
        if match:
            hops.add(int(match.group(1)))
    return sorted(hops)

def generate_html(ip, description):
    log_path = os.path.join(LOG_DIR, f"{ip}.log")
    trace_path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    html_path = os.path.join(HTML_DIR, f"{ip}.html")

    logs = []
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                logs = [line.strip() for line in f if line.strip()]
                logs = logs[-LOG_LINES_DISPLAY:][::-1]
        except Exception as e:
            logger.warning(f"Could not read logs for {ip}: {e}")

    traceroute = []
    if os.path.exists(trace_path):
        try:
            with open(trace_path) as f:
                traceroute = f.read().splitlines()
        except Exception as e:
            logger.warning(f"Could not read traceroute for {ip}: {e}")

    hops = get_available_hops(ip)
    time_labels = [tr["label"] for tr in TIME_RANGES]

    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>\n")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>\n")
            f.write(f"<title>{ip}</title>\n")
            f.write("""
<style>
body { font-family: Arial; margin: 20px; background: #f9f9f9; }
.graph-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; }
.tab-button { padding: 5px 10px; margin: 2px; cursor: pointer; border: 1px solid #ccc; border-radius: 4px; }
.tab-button.active { background-color: #333; color: white; }
.graph-img { display: none; }
</style>
<script>
function setTimeRange(ip, range) {
    document.querySelectorAll(`.graph-img-${ip}`).forEach(img => {
        img.style.display = (img.dataset.range === range) ? 'block' : 'none';
    });
    document.querySelectorAll(`.tab-${ip}`).forEach(btn => {
        btn.classList.remove('active');
    });
    document.getElementById(`tab-${ip}-${range}`).classList.add('active');
}
</script>
</head><body>
""")
            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"<p><i>Generated: {generated_time}</i></p>")

            # Traceroute
            if traceroute:
                f.write("<h3>Traceroute</h3><table><tr><th>Hop</th><th>Address</th><th>Details</th></tr>")
                for line in traceroute:
                    parts = line.strip().split()
                    hop = parts[0] if len(parts) > 0 else "?"
                    ipaddr = parts[1] if len(parts) > 1 else "?"
                    latency = " ".join(parts[2:]) if len(parts) > 2 else "-"
                    f.write(f"<tr><td>{hop}</td><td>{ipaddr}</td><td>{latency}</td></tr>")
                f.write("</table>")

            # Global time range selector
            f.write("<h3>Graphs</h3><p>Time Range:")
            for label in time_labels:
                f.write(f" <button class='tab-button tab-{ip}' id='tab-{ip}-{label}' onclick=\"setTimeRange('{ip}','{label}')\">{label.upper()}</button>")
            f.write("</p>")

            # Summary graphs (multi-hop)
            f.write("<h4>Summary (All Hops)</h4><div class='graph-grid'>")
            for metric in ["avg", "last", "best", "loss"]:
                for label in time_labels:
                    filename = f"{ip}_{metric}_{label}.png"
                    if os.path.exists(os.path.join(GRAPH_DIR, filename)):
                        f.write(f"<div class='graph-img graph-img-{ip}' data-range='{label}'><img src='graphs/{filename}'><br>{metric.upper()}</div>")
            f.write("</div>")

            # Per-hop graphs
            for hop in hops:
                f.write(f"<h4>Hop {hop}</h4><div class='graph-grid'>")
                for metric in ["avg", "last", "best", "loss"]:
                    for label in time_labels:
                        filename = f"{ip}_hop{hop}_{metric}_{label}.png"
                        if os.path.exists(os.path.join(GRAPH_DIR, filename)):
                            f.write(f"<div class='graph-img graph-img-{ip}' data-range='{label}'><img src='graphs/{filename}'><br>{metric.upper()}</div>")
                f.write("</div>")

            # Logs
            f.write("<h3>Recent Logs</h3><pre>")
            for line in logs:
                f.write(line + "\n")
            f.write("</pre><hr><a href='index.html'>Back to index</a></body></html>")
        logger.info(f"Generated HTML page for {ip}")
    except Exception as e:
        logger.exception(f"Failed to write HTML for {ip}: {e}")

for target in targets:
    generate_html(target["ip"], target.get("description", ""))
