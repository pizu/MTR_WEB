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

    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>")
            f.write(f"<title>{ip}</title>")
            f.write("""
<style>
    body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
    h2 { margin-top: 0; }
    table { border-collapse: collapse; }
    th, td { padding: 5px 10px; border: 1px solid #ccc; }
    .graph-section { margin-bottom: 25px; border: 1px solid #ddd; padding: 10px; background: #fff; }
    .graph-header { display: flex; justify-content: space-between; align-items: center; }
    .graph-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; margin-top: 10px; }
    .hidden { display: none; }
    .log-box { max-height: 300px; overflow-y: auto; background: #1e1e1e; color: #d4d4d4; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 13px; }
    .log-line { white-space: pre-wrap; }
</style>
<script>
function toggleSection(id) {
    const el = document.getElementById(id);
    el.classList.toggle('hidden');
}

function switchGraph(ip, metric, selected) {
    document.querySelectorAll(`.graph-img-${metric}-${ip}`).forEach(el => {
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
</script>
""")
            f.write("</head><body>")

            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            f.write(f"<p><i>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i></p>")

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
            time_labels = [tr["label"] for tr in TIME_RANGES]
            for metric in ["avg", "last", "best", "loss"]:
                section_id = f"section-{ip}-{metric}"
                f.write(f"<div class='graph-section'>")
                f.write(f"<div class='graph-header'><h4>{metric.upper()} Graphs</h4>")
                f.write(f"<button onclick=\"toggleSection('{section_id}')\">Toggle</button></div>")
                f.write(f"<div id='{section_id}' class=''>")
                f.write(f"<label>Time Range: </label>")
                f.write(f"<select onchange=\"switchGraph('{ip}', '{metric}', this.value)\">")
                for i, label in enumerate(time_labels):
                    selected = "selected" if i == 0 else ""
                    f.write(f"<option value='{label}' {selected}>{label.upper()}</option>")
                f.write("</select>")
                f.write("<div class='graph-grid'>")
                for i, label in enumerate(time_labels):
                    img_filename = f"{ip}_{metric}_{label}.png"
                    img_path = os.path.join(GRAPH_DIR, img_filename)
                    if os.path.exists(img_path):
                        display = "block" if i == 0 else "none"
                        f.write(f"<div style='display:{display}' class='graph-img-{metric}-{ip}' data-range='{label}'>")
                        f.write(f"<img src='graphs/{img_filename}' alt='{metric} graph {label}' loading='lazy'>")
                        f.write("</div>")
                f.write("</div></div></div>")

            # Logs (raw and table)
            f.write(f"<h3>Recent Logs</h3>")
            f.write(f"<input type='text' id='logFilter' placeholder='Filter logs...' style='width:100%;margin-bottom:10px;padding:5px;' onkeyup='filterLogs()'>")

            f.write("<details open><summary style='cursor: pointer; font-weight: bold;'>Raw Log View</summary>")
            f.write("<div class='log-box'>")
            if logs:
                for line in logs:
                    css_style = ""
                    if "ERROR" in line:
                        css_style = "color: red;"
                    elif "WARNING" in line:
                        css_style = "color: orange;"
                    elif "INFO" in line:
                        css_style = "color: lightgreen;"
                    else:
                        css_style = "color: white;"
                    safe_line = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    f.write(f"<div class='log-line' style='{css_style}'>{safe_line}</div>\n")
            else:
                f.write("<div class='log-line'>No logs available.</div>")
            f.write("</div></details>")

            f.write("<details><summary style='cursor: pointer; font-weight: bold;'>Structured Table View</summary>")
            f.write("<table class='log-table' style='width:100%; border-collapse: collapse; font-family: monospace; font-size: 13px;'>")
            f.write("<thead><tr style='background-color:#333; color:white;'><th style='padding:5px;border:1px solid #ccc;'>Timestamp</th><th style='padding:5px;border:1px solid #ccc;'>Level</th><th style='padding:5px;border:1px solid #ccc;'>Message</th></tr></thead><tbody>")

            log_pattern = re.compile(r"(?:\[(.*?)\]\s*)?(?:\[(.*?)\]\s*)?(.*)")
            for line in logs:
                match = log_pattern.match(line)
                if match:
                    timestamp = match.group(1) or ""
                    level = match.group(2) or ""
                    message = match.group(3).strip()
                    row_color = {
                        "ERROR": "color: red;",
                        "WARNING": "color: orange;",
                        "INFO": "color: lightgreen;"
                    }.get(level, "color: white;")
                    message = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    f.write(f"<tr class='log-line'><td style='border:1px solid #ccc;padding:5px;'>{timestamp}</td><td style='border:1px solid #ccc;padding:5px;{row_color}'>{level}</td><td style='border:1px solid #ccc;padding:5px;'>{message}</td></tr>")
                else:
                    f.write(f"<tr class='log-line'><td colspan='3' style='border:1px solid #ccc;padding:5px;'>{line}</td></tr>")
            f.write("</tbody></table></details>")

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
