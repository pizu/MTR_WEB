#!/usr/bin/env python3
import os
import yaml
import subprocess
from datetime import datetime
from utils import load_settings, setup_logger

# Load settings and initialize logger
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("index_generator", log_directory, "index_generator.log")

LOG_DIR = settings.get("log_directory", "logs")
HTML_DIR = "html"

# Load targets from YAML
try:
    with open("mtr_targets.yaml") as f:
        targets = yaml.safe_load(f).get("targets", [])
        logger.info(f"Loaded {len(targets)} targets from mtr_targets.yaml")
except Exception as e:
    logger.error(f"Failed to load targets from mtr_targets.yaml: {e}")
    targets = []

# Generate index.html
index_path = os.path.join(HTML_DIR, "index.html")
try:
    with open(index_path, "w") as f:
        f.write("""<html>
<head>
    <meta charset='utf-8'>
    <title>MTR Monitoring</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f8f9fa; }
        h2 { color: #333; }
        table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        th, td { border: 1px solid #ccc; padding: 12px 15px; text-align: left; }
        th { background-color: #f0f0f0; cursor: pointer; }
        tr:hover { background-color: #f5f5f5; }
        td a { text-decoration: none; color: #007bff; }
        td a:hover { text-decoration: underline; }
        .footer { margin-top: 20px; font-size: 0.9em; color: #666; }
        #filterInput { margin-bottom: 10px; padding: 5px; width: 200px; }
    </style>
    <script>
        function sortTable(n) {
            const table = document.getElementById("targetTable");
            let switching = true, shouldSwitch, dir = "asc", switchcount = 0;
            while (switching) {
                switching = false;
                const rows = table.rows;
                for (let i = 1; i < (rows.length - 1); i++) {
                    shouldSwitch = false;
                    const x = rows[i].getElementsByTagName("TD")[n];
                    const y = rows[i + 1].getElementsByTagName("TD")[n];
                    if (dir == "asc" && x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
                        shouldSwitch = true; break;
                    } else if (dir == "desc" && x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
                        shouldSwitch = true; break;
                    }
                }
                if (shouldSwitch) {
                    rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                    switching = true;
                    switchcount++;
                } else if (switchcount === 0 && dir === "asc") {
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

        setTimeout(() => location.reload(), 60000);
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
</tr>
""")

        for t in targets:
            ip = t.get("ip")
            description = t.get("description", "")
            log_path = os.path.join(LOG_DIR, f"{ip}.log")

            status = "N/A"
            last_seen = "Never"

            if os.path.exists(log_path):
                try:
                    with open(log_path) as logf:
                        lines = [line.strip() for line in logf if "MTR RUN" in line]
                        last_seen_line = lines[-1] if lines else ""
                        last_seen = last_seen_line.split("]")[0].strip("[") if last_seen_line else "Never"
                except Exception as e:
                    logger.warning(f"Could not read log for {ip}: {e}")

            # Check IP reachability using fping
            try:
                subprocess.run(["fping", "-c1", "-t500", ip], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                status = "Reachable"
            except subprocess.CalledProcessError:
                status = "Unreachable"

            f.write("<tr>")
            f.write(f"<td><a href='{ip}.html'>{ip}</a></td>")
            f.write(f"<td>{description}</td>")
            f.write(f"<td>{status}</td>")
            f.write(f"<td>{last_seen}</td>")
            f.write("</tr>\n")

            logger.info(f"Processed {ip} — Status: {status}, Last Seen: {last_seen}")

        f.write(f"""</table>
<div class='footer'>Generated at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} — Auto-refresh every 60s</div>
</body>
</html>""")

    logger.info(f"[UPDATED] index.html with {len(targets)} targets")

except Exception as e:
    logger.error(f"Failed to generate index.html: {e}")
