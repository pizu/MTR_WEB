#!/usr/bin/env python3
import os
import yaml
import subprocess
from datetime import datetime
from shutil import which
import rrdtool
from utils import load_settings, setup_logger

# Load settings and logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("index_generator", log_directory, "index_generator.log", settings=settings)

LOG_DIR = settings.get("log_directory", "logs")
HTML_DIR = "html"
RRD_DIR = settings.get("rrd_directory", "rrd")
ENABLE_FPING = settings.get("enable_fping_check", True)
REFRESH_SECONDS = settings.get("html_auto_refresh_seconds", 0)
FPING_PATH = settings.get("fping_path", which("fping"))
DATA_SOURCES = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", [])]
MAX_HOPS = settings.get("max_hops", 30)

# Validate fping
if ENABLE_FPING and not FPING_PATH:
    logger.warning("'fping' not found. Please configure 'fping_path' or install it.")
    ENABLE_FPING = False
else:
    logger.info(f"Using fping from: {FPING_PATH}")

# Load targets
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f).get("targets", [])
        logger.info(f"Loaded {len(targets)} targets")
except Exception as e:
    logger.error(f"Failed to load targets: {e}")
    targets = []

# Extract latest RRD data
def get_rrd_metrics(ip):
    rrd_path = os.path.join(RRD_DIR, f"{ip}.rrd")
    if not os.path.exists(rrd_path):
        return {}, {}

    try:
        end = int(datetime.now().timestamp())
        start = end - 120  # grab last 2 mins
        fetch_result = rrdtool.fetch(rrd_path, "AVERAGE", "--start", str(start), "--end", str(end))
        (start_ts, end_ts, step), ds_names, rows = fetch_result

        if not rows:
            return {}, {}

        latest_row = next((row for row in reversed(rows) if any(v is not None for v in row)), None)
        if not latest_row:
            return {}, {}

        hop0_metrics = {}
        total_metrics = {name: [] for name in DATA_SOURCES}

        for i, ds_name in enumerate(ds_names):
            value = latest_row[i]
            if value is None:
                continue

            parts = ds_name.split("_")
            if len(parts) != 2:
                continue

            hop_id, metric = parts
            if hop_id == "hop0":
                hop0_metrics[metric] = round(value, 1)
            total_metrics[metric].append(value)

        # Aggregate total metrics (mean across hops)
        avg_metrics = {}
        for metric, vals in total_metrics.items():
            if not vals:
                continue
            if metric == "loss":
                avg_metrics[metric] = round(sum(vals), 1)  # total % loss
            else:
                avg_metrics[metric] = round(sum(vals) / len(vals), 1)

        return hop0_metrics, avg_metrics

    except Exception as e:
        logger.warning(f"[RRD] Failed to extract metrics for {ip}: {e}")
        return {}, {}

# Generate index.html
index_path = os.path.join(HTML_DIR, "index.html")
try:
    with open(index_path, "w") as f:
        f.write("""<html>
<head>
    <meta charset='utf-8'>
""")
        if REFRESH_SECONDS > 0:
            f.write(f"    <meta http-equiv='refresh' content='{REFRESH_SECONDS}'>\n")
            logger.info(f"Auto-refresh enabled: {REFRESH_SECONDS}s")
        else:
            logger.info("Auto-refresh disabled")

        f.write("""    <title>MTR Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f8f9fa; }
        table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        th, td { border: 1px solid #ccc; padding: 10px 12px; text-align: left; }
        th { background-color: #f0f0f0; cursor: pointer; }
        tr:hover { background-color: #f5f5f5; }
        td a { text-decoration: none; color: #007bff; }
        td a:hover { text-decoration: underline; }
        .footer { margin-top: 20px; font-size: 0.9em; color: #666; }
        #filterInput { margin-bottom: 10px; padding: 5px; width: 250px; }
    </style>
    <script>
        function sortTable(n) {
            const table = document.getElementById("targetTable");
            let switching = true, dir = "asc", switchcount = 0;
            while (switching) {
                switching = false;
                const rows = table.rows;
                for (let i = 1; i < (rows.length - 1); i++) {
                    const x = rows[i].getElementsByTagName("TD")[n];
                    const y = rows[i + 1].getElementsByTagName("TD")[n];
                    if ((dir == "asc" && x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) ||
                        (dir == "desc" && x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase())) {
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchcount++;
                    }
                }
                if (switchcount === 0 && dir === "asc") {
                    dir = "desc"; switching = true;
                }
            }
        }

        function filterTable() {
            const input = document.getElementById("filterInput").value.toLowerCase();
            const rows = document.getElementById("targetTable").rows;
            for (let i = 1; i < rows.length; i++) {
                const row = rows[i].textContent.toLowerCase();
                rows[i].style.display = row.includes(input) ? "" : "none";
            }
        }
    </script>
</head>
<body>
<h2>MTR Monitoring Dashboard</h2>
<input type="text" id="filterInput" onkeyup="filterTable()" placeholder="Filter IP or description...">
<table id="targetTable">
<tr>
    <th onclick="sortTable(0)">IP</th>
    <th onclick="sortTable(1)">Description</th>
    <th onclick="sortTable(2)">Status</th>
    <th onclick="sortTable(3)">Last Seen</th>
    <th onclick="sortTable(4)">Hop0</th>
    <th onclick="sortTable(5)">Total</th>
</tr>
""")

        for target in targets:
            ip = target.get("ip")
            description = target.get("description", "")
            log_path = os.path.join(LOG_DIR, f"{ip}.log")
            status = "Unknown"
            last_seen = "Never"

            if os.path.exists(log_path):
                try:
                    with open(log_path) as logf:
                        lines = [line.strip() for line in logf if "MTR RUN" in line]
                        if lines:
                            last_seen = lines[-1].split("]")[0].strip("[")
                except Exception as e:
                    logger.warning(f"Could not read log for {ip}: {e}")

            if ENABLE_FPING:
                try:
                    result = subprocess.run([FPING_PATH, "-c1", "-t500", ip],
                                            stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL)
                    status = "Reachable" if result.returncode == 0 else "Unreachable"
                except Exception as e:
                    logger.warning(f"fping failed for {ip}: {e}")
                    status = "Unknown"

            hop0_metrics, avg_metrics = get_rrd_metrics(ip)

            def fmt_metrics(metrics):
                return ", ".join(f"{k}: {v}" for k, v in metrics.items()) if metrics else "-"

            f.write("<tr>")
            f.write(f"<td><a href='{ip}.html'>{ip}</a></td>")
            f.write(f"<td>{description}</td>")
            f.write(f"<td>{status}</td>")
            f.write(f"<td>{last_seen}</td>")
            f.write(f"<td>{fmt_metrics(hop0_metrics)}</td>")
            f.write(f"<td title='Aggregated across all hops'>{fmt_metrics(avg_metrics)}</td>")
            f.write("</tr>\n")

        f.write(f"""</table>
<div class='footer'>Generated at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} — Auto-refresh: {'enabled' if REFRESH_SECONDS > 0 else 'disabled'}</div>
</body>
</html>""")
        logger.info(f"index.html generated with {len(targets)} rows")

except Exception as e:
    logger.error(f"Failed to generate index.html: {e}")
