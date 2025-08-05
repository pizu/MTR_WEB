#!/usr/bin/env python3
import os
import re
from datetime import datetime
from utils import load_settings, setup_logger

# Global config and logger
settings = load_settings()
logger = setup_logger("html_builder", settings.get("log_directory", "/tmp"), "html_builder.log", settings=settings)

def generate_target_html(ip, description, hops):
    LOG_DIR = settings.get("log_directory", "logs")
    GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
    HTML_DIR = "html"
    TRACEROUTE_DIR = settings.get("traceroute_directory", "traceroute")
    TIME_RANGES = settings.get("graph_time_ranges", [{"label": "1h", "seconds": 3600}])
    REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)
    LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)

    safe_ip = ip.replace('.', '_')
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

    time_labels = [tr["label"] for tr in TIME_RANGES]

    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>")
            f.write(f"<title>{ip}</title>")
            f.write("""<style>
body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
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
    const safeIp = ip.replaceAll('.', '_');
    ['avg', 'last', 'best', 'loss'].forEach(metric => {
        document.querySelectorAll(`.graph-img-global-${safeIp}-${metric}`).forEach(el => {
            el.style.display = (el.dataset.range === selected) ? 'block' : 'none';
        });
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
</head><body>""")

            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            refresh_note = f"Auto-refresh every {REFRESH_SECONDS}s" if REFRESH_SECONDS > 0 else "Auto-refresh disabled"
            f.write(f"<p><i>Generated: {generated_time} — {refresh_note}</i></p>")

            if traceroute:
                f.write("<h3>Traceroute</h3><table><tr><th>Hop</th><th>Address</th><th>Details</th></tr>")
                for idx, line in enumerate(traceroute, start=1):
                    parts = line.strip().split()
                    hop_ip = parts[1] if len(parts) >= 2 else "???"
                    latency = parts[2] + " " + parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else "-")
                    if hop_ip == "???":
                        hop_ip = "Request timed out"
                        latency = "-"
                    f.write(f"<tr><td>{idx}</td><td>{hop_ip}</td><td>{latency}</td></tr>")
                f.write("</table>")

            f.write("<h3>Graphs</h3>")
            f.write("<label>Time Range: </label>")
            f.write(f"<select onchange=\"setGlobalTimeRange('{ip}', this.value)\">")
            for i, label in enumerate(time_labels):
                selected = "selected" if i == 0 else ""
                f.write(f"<option value='{label}' {selected}>{label.upper()}</option>")
            f.write("</select>")

            for metric in ["avg", "last", "best", "loss"]:
                section_id = f"summary-{safe_ip}-{metric}"
                f.write(f"<div class='graph-section'><div class='graph-header'><h4>{metric.upper()} Summary</h4>")
                f.write(f"<button onclick=\"toggleSection('{section_id}')\">Toggle</button></div>")
                f.write(f"<div id='{section_id}' class=''><div class='graph-grid'>")
                for i, label in enumerate(time_labels):
                    filename = f"{ip}_{metric}_{label}.png"
                    if os.path.exists(os.path.join(GRAPH_DIR, filename)):
                        display = "block" if i == 0 else "none"
                        f.write(f"<div style='display:{display}' class='graph-img-global-{safe_ip}-{metric}' data-range='{label}'>")
                        f.write(f"<img src='graphs/{filename}' alt='{metric} summary {label}' loading='lazy'>")
                        f.write("</div>")
                f.write("</div></div></div>")

            f.write("<h4>Per-Hop Graphs</h4>")
            f.write(f"<p><a href='{ip}_hops.html' target='_blank'><button>Open Per-Hop Graphs</button></a></p>")

            f.write("<h3>Recent Logs</h3>")
            f.write("<input type='text' id='logFilter' placeholder='Filter logs...' style='width:100%;margin-bottom:10px;padding:5px;' onkeyup='filterLogs()'>")
            f.write("<table class='log-table'><thead><tr><th>Timestamp</th><th>Level</th><th>Message</th></tr></thead><tbody>")

            ts_re = re.compile(r"\[(.*?)\]\s*(.*)")
            for line in logs:
                ts, msg, level = "", line, ""
                m = ts_re.match(line)
                if m:
                    ts = m.group(1).strip()
                    msg = m.group(2).strip()
                lmsg = msg.lower()
                if "loss" in lmsg:
                    level = "WARNING"
                elif "hop path" in lmsg or "mtr run" in lmsg:
                    level = "INFO"
                elif "error" in lmsg:
                    level = "ERROR"
                color = {"ERROR": "color:red;", "WARNING": "color:orange;", "INFO": "color:lightgreen;"}.get(level, "color:white;")
                msg = msg.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                f.write(f"<tr class='log-line'><td>{ts}</td><td style='{color}'>{level}</td><td>{msg}</td></tr>")

            f.write("</tbody></table><hr><p><a href='index.html'>Back to index</a></p></body></html>")
        logger.info(f"Generated HTML page: {html_path}")
        generate_per_hop_html(ip, hops, description)
    except Exception as e:
        logger.exception(f"[{ip}] Failed to generate HTML")


def generate_per_hop_html(ip, hops, description):
    GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
    HTML_DIR = "html"
    TIME_RANGES = settings.get("graph_time_ranges", [{"label": "1h", "seconds": 3600}])
    html_path = os.path.join(HTML_DIR, f"{ip}_hops.html")
    safe_ip = ip.replace('.', '_')

    try:
        with open(html_path, "w") as f:
            f.write("<html><head><meta charset='utf-8'>")
            f.write(f"<title>Per-Hop Graphs — {ip}</title>")
            f.write("""<style>
body { font-family: Arial; margin: 20px; background: #f4f4f4; }
.graph-section { margin-bottom: 25px; border: 1px solid #ccc; padding: 10px; background: #fff; }
.graph-header { display: flex; justify-content: space-between; align-items: center; }
.graph-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 10px; margin-top: 10px; }
.hidden { display: none; }
</style>
<script>
function setHopTimeRange(ip, hop, selected) {
    const safeIp = ip.replaceAll('.', '_');
    document.querySelectorAll(`.hop-graph-${safeIp}-${hop}`).forEach(el => {
        el.style.display = (el.dataset.range === selected) ? 'block' : 'none';
    });
}
function toggleHopMetrics(ip, hop) {
    const safeIp = ip.replaceAll('.', '_');
    document.querySelectorAll(`.hop-metric-${safeIp}-${hop}`).forEach(el => {
        el.classList.toggle('hidden');
    });
}
</script>
</head><body>""")

            f.write(f"<h2>Per-Hop Graphs — {ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")
            f.write(f"<p><a href='{ip}.html'>← Back to main page</a></p>")

            if not hops:
                f.write("<p><i>No per-hop graphs available.</i></p>")
                logger.warning(f"[{ip}] No hops found for per-hop HTML.")
                return

            for hop in hops:
                f.write(f"<div class='graph-section'><div class='graph-header'><h3>Hop {hop}</h3>")
                f.write(f"<button onclick=\"toggleHopMetrics('{ip}', {hop})\">Toggle Metrics</button></div>")
                f.write(f"<label>Time Range: </label><select onchange=\"setHopTimeRange('{ip}', {hop}, this.value)\">")
                for i, label in enumerate(TIME_RANGES):
                    selected = "selected" if i == 0 else ""
                    f.write(f"<option value='{label['label']}' {selected}>{label['label'].upper()}</option>")
                f.write("</select>")

                for metric in ["avg", "last", "best", "loss"]:
                    f.write(f"<div class='graph-grid hop-metric-{safe_ip}-{hop}'>")
                    for i, label in enumerate(TIME_RANGES):
                        png = f"{ip}_hop{hop}_{metric}_{label['label']}.png"
                        if os.path.exists(os.path.join(GRAPH_DIR, png)):
                            display = "block" if i == 0 else "none"
                            f.write(f"<div style='display:{display}' class='hop-graph-{safe_ip}-{hop}' data-range='{label['label']}'>")
                            f.write(f"<img src='graphs/{png}' alt='Hop {hop} {metric} {label['label']}' loading='lazy'>")
                            f.write("</div>")
                    f.write("</div>")  # graph-grid
                f.write("</div>")  # graph-section

            f.write("</body></html>")
        logger.info(f"[{ip}] Per-hop HTML generated: {html_path}")
    except Exception as e:
        logger.exception(f"[{ip}] Failed to generate per-hop HTML")
