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
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f)["targets"]
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

# Template function
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

    # Detect hop indices
    hop_indices = []
    for file in os.listdir(GRAPH_DIR):
        match = re.match(rf"{re.escape(ip)}_hop(\d+)_avg_", file)
        if match:
            idx = int(match.group(1))
            if idx not in hop_indices:
                hop_indices.append(idx)
    hop_indices.sort()

    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>\n")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>\n")
                logger.info(f"[{ip}] Auto-refresh enabled: {REFRESH_SECONDS} seconds")
            else:
                logger.info(f"[{ip}] Auto-refresh disabled")

            f.write(f"<title>{ip}</title>\n")
            f.write("""<style>
body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
h2 { margin-top: 0; }
table { border-collapse: collapse; width: 100%; }
th, td { padding: 5px 10px; border: 1px solid #ccc; }
.graph-section { margin-bottom: 25px; border: 1px solid #ddd; padding: 10px; background: #fff; }
.graph-header { display: flex; justify-content: space-between; align-items: center; }
.graph-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; margin-top: 10px; }
.hidden { display: none; }
.log-line { white-space: pre-wrap; }
</style>
<script>
function toggleSection(id) {
    const el = document.getElementById(id);
    el.classList.toggle('hidden');
}
function setGlobalTimeRange(ip, selected) {
    document.querySelectorAll(`.graph-img-global-${ip}`).forEach(el => {
        el.style.display = (el.dataset.range === selected) ? 'block' : 'none';
    });
}
function filterLogs() {
    const input = document.getElementById('logFilter').value.toLowerCase();
    const lines = document.getElementsByClassName('log-line');
    for (const line of lines) {
        line.style.display = line.innerText.toLowerCase().includes(input) ? '' : 'none';
    }
}
</script>""")
            f.write("</head><body>")

            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            refresh_note = f"Auto-refresh every {REFRESH_SECONDS}s" if REFRESH_SECONDS > 0 else "Auto-refresh disabled"
            f.write(f"<p><i>Generated: {generated_time} — {refresh_note}</i></p>")
            logger.info(f"[{ip}] Generated HTML page at {generated_time} — {refresh_note}")

            if traceroute:
                f.write("<h3>Traceroute</h3>")
                f.write("<table><tr><th>Hop</th><th>Address / Hostname</th><th>Details</th></tr>")
                for line in traceroute:
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        hop = parts[0]
                        ipaddr = parts[1]
                        latency = parts[2] + " " + parts[3] if len(parts) > 3 else parts[2]
                        if ipaddr == "???":
                            ipaddr = "Request timed out"
                            latency = "-"
                    else:
                        hop = "?"
                        ipaddr = line.strip()
                        latency = "-"
                    f.write(f"<tr><td>{hop}</td><td>{ipaddr}</td><td>{latency}</td></tr>")
                f.write("</table>")
                f.write(f"<p><a href='../{TRACEROUTE_DIR}/{ip}.trace.txt' target='_blank'>Download traceroute text</a></p>")
            else:
                f.write("<p><i>No traceroute data available.</i></p>")

            f.write("<h3>Graphs</h3>")
            f.write("<label>Time Range: </label>")
            f.write(f"<select onchange=\"setGlobalTimeRange('{ip}', this.value)\">")
            for i, label in enumerate(time_labels):
                selected = "selected" if i == 0 else ""
                f.write(f"<option value='{label}' {selected}>{label.upper()}</option>")
            f.write("</select>")

            # --- Summary Graphs (all hops in one chart) ---
            f.write("<h4>Summary (All Hops)</h4>")
            for metric in ["avg", "last", "best", "loss"]:
                section_id = f"summary-{ip}-{metric}"
                f.write(f"<div class='graph-section'>")
                f.write(f"<div class='graph-header'><h4>{metric.upper()} Summary</h4>")
                f.write(f"<button onclick=\"toggleSection('{section_id}')\">Toggle</button></div>")
                f.write(f"<div id='{section_id}' class=''>")
                f.write("<div class='graph-grid'>")
                for i, label in enumerate(time_labels):
                    filename = f"{ip}_{metric}_{label}.png"
                    full_path = os.path.join(GRAPH_DIR, filename)
                    if os.path.exists(full_path):
                        display = "block" if i == 0 else "none"
                        f.write(f"<div style='display:{display}' class='graph-img-global-{ip}' data-range='{label}'>")
                        f.write(f"<img src='graphs/{filename}' alt='{metric} summary {label}' loading='lazy'>")
                        f.write("</div>")
                f.write("</div></div></div>")  # end graph-grid, div, section

            # --- Per-Hop Graphs ---
            f.write("<h4>Per-Hop Graphs</h4>")
            for hop in hops:
                section_id = f"{ip}-hop{hop}"
                f.write(f"<div class='graph-section'>")
                f.write(f"<div class='graph-header'><h4>Hop {hop}</h4>")
                f.write(f"<button onclick=\"toggleSection('{section_id}')\">Toggle</button></div>")
                f.write(f"<div id='{section_id}' class=''>")

                # Dropdown for per-hop time range
                f.write(f"<label>Time Range: </label>")
                f.write(f"<select onchange=\"setHopTimeRange('{ip}', {hop}, this.value)\">")
                for i, label in enumerate(time_labels):
                    selected = "selected" if i == 0 else ""
                    f.write(f"<option value='{label}' {selected}>{label.upper()}</option>")
                f.write("</select>")

                # Graphs per metric per time
                for metric in ["avg", "last", "best", "loss"]:
                    f.write(f"<div class='graph-grid'>")
                    for i, label in enumerate(time_labels):
                        filename = f"{ip}_hop{hop}_{metric}_{label}.png"
                        full_path = os.path.join(GRAPH_DIR, filename)
                        if os.path.exists(full_path):
                            display = "block" if i == 0 else "none"
                            f.write(f"<div style='display:{display}' class='hop-graph hop-graph-{ip}-{hop}' data-range='{label}'>")
                            f.write(f"<img src='graphs/{filename}' alt='Hop {hop} {metric} {label}' loading='lazy'>")
                            f.write("</div>")
                    f.write("</div>")  # end graph-grid
                f.write("</div></div>")  # end hop section

            # JS to control hop graph switching
            f.write("""
<script>
function setGlobalTimeRange(ip, selected) {
    document.querySelectorAll(`.graph-img-global-${ip}`).forEach(el => {
        el.style.display = (el.dataset.range === selected) ? 'block' : 'none';
    });
}
function setHopTimeRange(ip, hop, selected) {
    document.querySelectorAll(`.hop-graph-${ip}-${hop}`).forEach(el => {
        el.style.display = (el.dataset.range === selected) ? 'block' : 'none';
    });
}
</script>
""")

            f.write("<h3>Recent Logs</h3>")
            f.write("<input type='text' id='logFilter' placeholder='Filter logs...' style='width:100%;margin-bottom:10px;padding:5px;' onkeyup='filterLogs()'>")
            f.write("<table class='log-table'><thead><tr style='background-color:#333; color:white;'>")
            f.write("<th style='padding:5px;border:1px solid #ccc;'>Timestamp</th>")
            f.write("<th style='padding:5px;border:1px solid #ccc;'>Level</th>")
            f.write("<th style='padding:5px;border:1px solid #ccc;'>Message</th></tr></thead><tbody>")

            timestamp_pattern = re.compile(r"\[(.*?)\]\s*(.*)")
            for line in logs:
                timestamp = ""
                message = line
                level = ""

                match = timestamp_pattern.match(line)
                if match:
                    timestamp = match.group(1).strip()
                    message = match.group(2).strip()

                lower_msg = message.lower()
                if "loss" in lower_msg:
                    level = "WARNING"
                elif "hop path changed" in lower_msg:
                    level = "INFO"
                elif "mtr run" in lower_msg:
                    level = "INFO"
                elif "error" in lower_msg:
                    level = "ERROR"

                row_color = {
                    "ERROR": "color: red;",
                    "WARNING": "color: orange;",
                    "INFO": "color: lightgreen;"
                }.get(level, "color: white;")

                message = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                f.write(f"<tr class='log-line'><td style='border:1px solid #ccc;padding:5px;'>{timestamp}</td>")
                f.write(f"<td style='border:1px solid #ccc;padding:5px;{row_color}'>{level}</td>")
                f.write(f"<td style='border:1px solid #ccc;padding:5px;'>{message}</td></tr>")

            f.write("</tbody></table>")
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
