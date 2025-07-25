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

def generate_per_hop_html(ip, hops, description):
    html_path = os.path.join(HTML_DIR, f"{ip}_hops.html")
    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>")
            f.write(f"<title>Per-Hop Graphs — {ip}</title>")
            f.write("""<style>
body { font-family: Arial; margin: 20px; background: #f4f4f4; }
.graph-section { margin-bottom: 25px; border: 1px solid #ccc; padding: 10px; background: #fff; }
.graph-header { display: flex; justify-content: space-between; align-items: center; }
.graph-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 10px; margin-top: 10px; }
</style><script>
function setHopTimeRange(ip, hop, selected) {
    document.querySelectorAll(`.hop-graph-${ip}-${hop}`).forEach(el => {
        el.style.display = (el.dataset.range === selected) ? 'block' : 'none';
    });
}
</script></head><body>""")

            f.write(f"<h2>Per-Hop Graphs — {ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            f.write(f"<p><a href='{ip}.html'>← Back to main page</a></p>")

            if not hops:
                f.write("<p><i>No per-hop graphs available.</i></p>")
                logger.warning(f"[{ip}] No hops found for per-hop HTML.")
            else:
                for hop in hops:
                    f.write(f"<div class='graph-section'><div class='graph-header'><h3>Hop {hop}</h3></div>")

                    f.write(f"<label>Time Range: </label>")
                    f.write(f"<select onchange=\"setHopTimeRange('{ip}', {hop}, this.value)\">")
                    for i, label in enumerate(TIME_RANGES):
                        selected = "selected" if i == 0 else ""
                        f.write(f"<option value='{label['label']}' {selected}>{label['label'].upper()}</option>")
                    f.write("</select>")

                    for metric in ["avg", "last", "best", "loss"]:
                        f.write("<div class='graph-grid'>")
                        for i, label in enumerate(TIME_RANGES):
                            filename = f"{ip}_hop{hop}_{metric}_{label['label']}.png"
                            full_path = os.path.join(GRAPH_DIR, filename)
                            if os.path.exists(full_path):
                                display = "block" if i == 0 else "none"
                                f.write(f"<div style='display:{display}' class='hop-graph-{ip}-{hop}' data-range='{label['label']}'>")
                                f.write(f"<img src='graphs/{filename}' alt='Hop {hop} {metric} {label['label']}' loading='lazy'>")
                                f.write("</div>")
                        f.write("</div>")  # graph-grid
                    f.write("</div>")  # graph-section
        logger.info(f"[{ip}] Per-hop HTML generated: {html_path}")
    except Exception as e:
        logger.exception(f"[{ip}] Failed to generate per-hop HTML")

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
    else:
        logger.warning(f"No traceroute file found for {ip} at {trace_path}")

    hops = get_available_hops(ip)
    logger.info(f"[{ip}] Found hops: {hops}")
    time_labels = [tr["label"] for tr in TIME_RANGES]

    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>")
            f.write(f"<title>{ip}</title>")
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
</script></head><body>""")

            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            refresh_note = f"Auto-refresh every {REFRESH_SECONDS}s" if REFRESH_SECONDS > 0 else "Auto-refresh disabled"
            f.write(f"<p><i>Generated: {generated_time} — {refresh_note}</i></p>")

            if traceroute:
                f.write("<h3>Traceroute</h3><table><tr><th>Hop</th><th>Address / Hostname</th><th>Details</th></tr>")
                for idx, line in enumerate(traceroute, start=1):
                    parts = line.strip().split()
                    ipaddr = parts[1] if len(parts) >= 2 else line.strip()
                    latency = parts[2] + " " + parts[3] if len(parts) > 3 else parts[2] if len(parts) > 2 else "-"
                    if ipaddr == "???":
                        ipaddr = "Request timed out"
                        latency = "-"
                    f.write(f"<tr><td>{idx}</td><td>{ipaddr}</td><td>{latency}</td></tr>")
                f.write("</table>")
                f.write(f"<p><a href='../{TRACEROUTE_DIR}/{ip}.trace.txt' target='_blank'>Download traceroute text</a></p>")

            f.write("<h3>Graphs</h3>")
            f.write("<label>Time Range: </label>")
            f.write(f"<select onchange=\"setGlobalTimeRange('{ip}', this.value)\">")
            for i, label in enumerate(time_labels):
                selected = "selected" if i == 0 else ""
                f.write(f"<option value='{label}' {selected}>{label.upper()}</option>")
            f.write("</select>")

            for metric in ["avg", "last", "best", "loss"]:
                section_id = f"summary-{ip}-{metric}"
                f.write(f"<div class='graph-section'><div class='graph-header'><h4>{metric.upper()} Summary</h4>")
                f.write(f"<button onclick=\"toggleSection('{section_id}')\">Toggle</button></div>")
                f.write(f"<div id='{section_id}' class=''><div class='graph-grid'>")
                for i, label in enumerate(time_labels):
                    filename = f"{ip}_{metric}_{label}.png"
                    if os.path.exists(os.path.join(GRAPH_DIR, filename)):
                        display = "block" if i == 0 else "none"
                        f.write(f"<div style='display:{display}' class='graph-img-global-{ip}' data-range='{label}'>")
                        f.write(f"<img src='graphs/{filename}' alt='{metric} summary {label}' loading='lazy'>")
                        f.write("</div>")
                f.write("</div></div></div>")

            # Link to hop page
            hop_page = f"{ip}_hops.html"
            f.write("<h4>Per-Hop Graphs</h4>")
            f.write(f"<p><a href='{hop_page}' target='_blank'><button>Open Per-Hop Graphs</button></a></p>")

            f.write("<h3>Recent Logs</h3>")
            f.write("<input type='text' id='logFilter' placeholder='Filter logs...' style='width:100%;margin-bottom:10px;padding:5px;' onkeyup='filterLogs()'>")
            f.write("<table class='log-table'><thead><tr style='background-color:#333; color:white;'>")
            f.write("<th style='padding:5px;border:1px solid #ccc;'>Timestamp</th>")
            f.write("<th style='padding:5px;border:1px solid #ccc;'>Level</th>")
            f.write("<th style='padding:5px;border:1px solid #ccc;'>Message</th></tr></thead><tbody>")

            ts_re = re.compile(r"\[(.*?)\]\s*(.*)")
            for line in logs:
                ts, msg, level = "", line, ""
                m = ts_re.match(line)
                if m:
                    ts = m.group(1).strip()
                    msg = m.group(2).strip()
                lmsg = msg.lower()
                if "loss" in lmsg: level = "WARNING"
                elif "hop path" in lmsg or "mtr run" in lmsg: level = "INFO"
                elif "error" in lmsg: level = "ERROR"
                color = {"ERROR": "color:red;", "WARNING": "color:orange;", "INFO": "color:lightgreen;"}.get(level, "color:white;")
                msg = msg.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                f.write(f"<tr class='log-line'><td>{ts}</td><td style='{color}'>{level}</td><td>{msg}</td></tr>")

            f.write("</tbody></table><hr><p><a href='index.html'>Back to index</a></p></body></html>")
        logger.info(f"Generated HTML page: {html_path}")
        generate_per_hop_html(ip, hops, description)
    except Exception as e:
        logger.exception(f"Failed to write main HTML for {ip}")

# Run for all targets
for target in targets:
    ip = target["ip"]
    desc = target.get("description", "")
    generate_html(ip, desc)

# Clean old files
try:
    all_html = [f for f in os.listdir(HTML_DIR) if f.endswith(".html") and f != "index.html"]
    for f in all_html:
        ip_clean = f.replace(".html", "").replace("_hops", "")
        if ip_clean not in target_ips:
            os.remove(os.path.join(HTML_DIR, f))
            logger.info(f"Removed stale HTML file: {f}")
except Exception as e:
    logger.warning(f"Failed to clean orphan HTML: {e}")
