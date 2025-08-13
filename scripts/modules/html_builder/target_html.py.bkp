#!/usr/bin/env python3
# modules/html_builder/target_html.py
#
# This version implements two dropdowns (Time Range + Metric) that control which
# summary graph image is shown. It relies on your existing filename convention:
#   html/graphs/{ip}_{metric}_{label}.png
# where:
#   - {ip}     is the target IP (e.g., "1.1.1.1")
#   - {metric} is one of the DS names from settings.rrd.data_sources[].name
#   - {label}  is one of the labels from settings.graph_time_ranges[].label
#
# All matching PNGs are laid out but initially hidden, and simple JS toggles
# visibility by matching data attributes (data-metric + data-range).
#
# Notes:
# - Reads time ranges from: settings["graph_time_ranges"]  (list of {label, seconds})
# - Reads metric list from: settings["rrd"]["data_sources"] (list of {name, ...})
# - Keeps logs + traceroute behavior identical to your current file.
# - Preserves your logger and paths behavior.

import os
import re
import html
from datetime import datetime
from modules.utils import load_settings, setup_logger
from modules.rrd_metrics import get_rrd_metrics  # kept for hop0 metric preview (small header hint)

def generate_target_html(ip, description, hops, settings):
    """
    Generates the HTML page for a target with:
    - Two dropdowns (Metric + Time Range)
    - One graph section whose content swaps based on dropdowns
    - Traceroute (from traceroute/<ip>.trace.txt)
    - Recent logs (last N lines, filterable in-page)
    """
    logger = setup_logger("target_html", settings.get("log_directory", "/tmp"), "target_html.log", settings=settings)

    LOG_DIR         = settings.get("log_directory", "logs")
    GRAPH_DIR       = settings.get("graph_output_directory", "html/graphs")
    HTML_DIR        = "html"
    TRACEROUTE_DIR  = settings.get("traceroute_directory", "traceroute")
    TIME_RANGES     = settings.get("graph_time_ranges", [{"label": "1h", "seconds": 3600}])
    REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)
    LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)
    RRD_DIR         = settings.get("rrd_directory", "rrd")

    # Metrics come from the configured RRD data sources (names only)
    DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]

    safe_ip   = ip.replace('.', '_')
    log_path  = os.path.join(LOG_DIR, f"{ip}.log")
    trace_path= os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    html_path = os.path.join(HTML_DIR, f"{ip}.html")

    # Read logs (tail last N and reverse so newest first)
    logs = []
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                logs = [line.strip() for line in f if line.strip()]
                logs = logs[-LOG_LINES_DISPLAY:][::-1]
        except Exception as e:
            logger.warning(f"Could not read logs for {ip}: {e}")

    # Read traceroute lines (plain text file)
    traceroute = []
    if os.path.exists(trace_path):
        try:
            with open(trace_path) as f:
                traceroute = f.read().splitlines()
        except Exception as e:
            logger.warning(f"Could not read traceroute for {ip}: {e}")

    # Extract the dropdown values
    time_labels   = [tr.get("label") for tr in TIME_RANGES if tr.get("label")]
    metrics_names = [m for m in DATA_SOURCES if isinstance(m, str) and m]

    # A small inline hint using RRD metrics (optional; if not available, it’s skipped)
    hop0_metrics, _ = get_rrd_metrics(ip, RRD_DIR, DATA_SOURCES)

    try:
        os.makedirs(HTML_DIR, exist_ok=True)
        with open(html_path, "w", encoding="utf-8") as f:
            # HTML HEAD
            f.write("<html><head><meta charset='utf-8'>")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>")
            f.write(f"<title>{ip}</title>")
            f.write("""<style>
body { font-family: Arial; margin: 20px; background: #f9f9f9; }
.graph-section { margin-bottom: 25px; border: 1px solid #ddd; padding: 10px; background: #fff; }
.graph-header { display: flex; flex-wrap: wrap; gap: 10px; justify-content: space-between; align-items: center; }
.graph-grid { display: grid; grid-template-columns: 1fr; gap: 10px; margin-top: 10px; }
.hidden { display: none; }
.log-line { white-space: pre-wrap; }
.log-table td { vertical-align: top; padding: 4px 6px; font-size: 13px; }
.log-table pre { margin: 0; max-height: 120px; overflow: auto; background-color: #f1f1f1; padding: 4px; border-radius: 4px; font-family: monospace; }
select { padding: 4px 8px; }
</style>
<script>
// Toggle visibility of graph PNGs by metric + time range
function setGraphFilters(ip) {
    const safeIp = ip.replaceAll('.', '_');
    const metricSel = document.getElementById('metricSelect');
    const rangeSel  = document.getElementById('timeRangeSelect');
    const metric = metricSel ? metricSel.value : '';
    const range  = rangeSel  ? rangeSel.value  : '';

    const imgs = document.getElementsByClassName('graph-img-' + safeIp);
    for (const el of imgs) {
        const m = el.getAttribute('data-metric');
        const r = el.getAttribute('data-range');
        el.style.display = (m === metric && r === range) ? 'block' : 'none';
    }
}

function initDefaults(ip) {
    const rangeSel  = document.getElementById('timeRangeSelect');
    const metricSel = document.getElementById('metricSelect');
    if (metricSel && metricSel.options.length > 0) metricSel.selectedIndex = 0;
    if (rangeSel  && rangeSel.options.length  > 0) rangeSel.selectedIndex  = 0;
    setGraphFilters(ip);
}

function toggleSection(id) {
    const el = document.getElementById(id);
    el.classList.toggle('hidden');
}

function filterLogs() {
    const input = document.getElementById('logFilter').value.toLowerCase();
    const lines = document.getElementsByClassName('log-line');
    for (const line of lines) {
        line.style.display = line.innerText.toLowerCase().includes(input) ? '' : 'none';
    }
}
</script>""")
            f.write("</head><body onload=\"initDefaults('%s')\">" % ip)

            # HEADER
            f.write(f"<h2>{ip}</h2>")
            if description:
                f.write(f"<p><b>{description}</b></p>")

            if hop0_metrics:
                metrics_str = ", ".join(f"{k}: {v}" for k, v in hop0_metrics.items())
                f.write(f"<p title='Last sample from RRD'>{metrics_str}</p>")

            generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            refresh_note   = f"Auto-refresh every {REFRESH_SECONDS}s" if REFRESH_SECONDS > 0 else "Auto-refresh disabled"
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

            # GRAPH CONTROLS + GRID
            f.write("<h3>Graphs</h3>")
            f.write("<div class='graph-section'>")
            f.write("<div class='graph-header'>")
            # Metric dropdown (from RRD DS names)
            f.write("<div><label>Metric:&nbsp;</label><select id='metricSelect' onchange=\"setGraphFilters('%s')\">" % ip)
            for m in metrics_names:
                f.write(f"<option value='{m}'>{m.upper()}</option>")
            f.write("</select></div>")
            # Time range dropdown (from settings.graph_time_ranges)
            f.write("<div><label>Time Range:&nbsp;</label><select id='timeRangeSelect' onchange=\"setGraphFilters('%s')\">" % ip)
            for label in time_labels:
                f.write(f"<option value='{label}'>{label.upper()}</option>")
            f.write("</select></div>")
            # Show/Hide button (if you like collapsing)
            f.write(f"<div><button onclick=\"toggleSection('graphs-{safe_ip}')\">Toggle</button></div>")
            f.write("</div>")  # .graph-header

            # All PNGs (one visible at a time) — we render any that exist
            f.write(f"<div id='graphs-{safe_ip}' class=''><div class='graph-grid'>")
            for m in metrics_names:
                for label in time_labels:
                    filename = f"{ip}_{m}_{label}.png"
                    png_path = os.path.join(GRAPH_DIR, filename)
                    if os.path.exists(png_path):
                        # Hidden by default; JS shows the one matching current selection
                        f.write(f"<div class='graph-img-{safe_ip}' data-metric='{m}' data-range='{label}' style='display:none'>")
                        f.write(f"<img src='graphs/{filename}' alt='{m} {label}' loading='lazy'></div>")
            f.write("</div></div></div>")  # end .graph-grid / .graph-section

            # LOGS
            f.write("<h3>Recent Logs</h3>")
            f.write("<input type='text' id='logFilter' placeholder='Filter logs...' style='width:100%;margin-bottom:10px;padding:5px;' onkeyup='filterLogs()'>")
            f.write("<table class='log-table'><thead><tr><th>Timestamp</th><th>Level</th><th>Message</th></tr></thead><tbody>")

            log_line_re = re.compile(r"^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) \\[(\\w+)\\] (.*)")
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

            f.write("</tbody></table><hr><p><a href='index.html'>Back to index</a></p>")
            f.write("</body></html>")

        logger.info(f"Generated HTML for {ip}")
    except Exception:
        logger.exception(f"[{ip}] Failed to generate target HTML")
