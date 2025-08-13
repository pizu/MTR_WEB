#!/usr/bin/env python3
# modules/html_builder/target_html.py

import os
import re
import html
from datetime import datetime
from modules.utils import load_settings, setup_logger
from modules.html_builder.per_hop_html import generate_per_hop_html
from modules.rrd_metrics import get_rrd_metrics

def generate_target_html(ip, description, hops, settings):
    """
    Generates the main HTML page for a target IP, showing:
    - Summary graphs (avg, loss, etc.)
    - Traceroute
    - Logs
    - Per-hop graph link

    It saves the file as: html/<ip>.html
    """
    logger = setup_logger("target_html", settings.get("log_directory", "/tmp"), "target_html.log", settings=settings)
    LOG_DIR = settings.get("log_directory", "logs")
    GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
    HTML_DIR = "html"
    TRACEROUTE_DIR = settings.get("traceroute_directory", "traceroute")
    TIME_RANGES = settings.get("graph_time_ranges", [{"label": "1h", "seconds": 3600}])
    REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)
    LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)
    RRD_DIR = settings.get("rrd_directory", "rrd")
    DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]

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
    hop0_metrics, _ = get_rrd_metrics(ip, RRD_DIR, DATA_SOURCES)

    try:
        with open(html_path, "w") as f:
            # START HTML
            f.write("<html><head><meta charset='utf-8'>")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>")
            f.write(f"<title>{ip}</title>")
            f.write("""<style>
body { font-family: Arial; margin: 20px; background: #f9f9f9; }
.graph-section { margin-bottom: 25px; border: 1px solid #ddd; padding: 10px; background: #fff; }
.graph-header { display: flex; justify-content: space-between; align-items: center; }
.graph-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; margin-top: 10px; }
.hidden { display: none; }
.log-line { white-space: pre-wrap; }
.log-table td { vertical-align: top; padding: 4px 6px; font-size: 13px; }
.log-table pre { margin: 0; max-height: 120px; overflow: auto; background-color: #f1f1f1; padding: 4px; border-radius: 4px; font-family: monospace; }
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
</script></head><body>""")

            # HEADER
            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")

            if hop0_metrics:
                metrics_str = ", ".join(f"{k}: {v}" for k, v in hop0_metrics.items())
                f.write(f"<p title='Last sample from RRD'>{metrics_str}</p>")

            generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            refresh_note = f"Auto-refresh every {REFRESH_SECONDS}s" if REFRESH_SECONDS > 0 else "Auto-refresh disabled"
            f.write(f"<p><i>Generated: {generated_time} — {refresh_note}</i></p>")

            # TRACEROUTE
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

            # TIME RANGE DROPDOWN
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
                f.write(f"<div id='{section_id}'><div class='graph-grid'>")
                for i, label in enumerate(time_labels):
                    filename = f"{ip}_{metric}_{label}.png"
                    if os.path.exists(os.path.join(GRAPH_DIR, filename)):
                        display = "block" if i == 0 else "none"
                        f.write(f"<div style='display:{display}' class='graph-img-global-{safe_ip}-{metric}' data-range='{label}'>")
                        f.write(f"<img src='graphs/{filename}' alt='{metric} {label}' loading='lazy'></div>")
                f.write("</div></div></div>")


            f.write("<h3>Recent Logs</h3>")
            f.write("<input type='text' id='logFilter' placeholder='Filter logs...' style='width:100%;margin-bottom:10px;padding:5px;' onkeyup='filterLogs()'>")
            f.write("<table class='log-table'><thead><tr><th>Timestamp</th><th>Level</th><th>Message</th></tr></thead><tbody>")

            log_line_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[(\w+)\] (.*)")
            for line in logs:
                match = log_line_re.match(line)
                ts, level, msg = match.groups() if match else ("", "", line)
                color = {
                    "DEBUG": "color:gray;",
                    "INFO": "color:lightgreen;",
                    "WARNING": "color:orange;",
                    "ERROR": "color:red;"
                }.get(level.upper(), "color:white;")
                msg = html.escape(msg)
                f.write(f"<tr class='log-line'><td>{ts}</td><td style='{color}'>{level}</td><td><pre>{msg}</pre></td></tr>")

            f.write("</tbody></table><hr><p><a href='index.html'>Back to index</a></p></body></html>")

        logger.info(f"Generated HTML for {ip}")
        generate_per_hop_html(ip, hops, description, settings)

    except Exception as e:
        logger.exception(f"[{ip}] Failed to generate target HTML")
