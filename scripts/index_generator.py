#!/usr/bin/env python3
import os
import yaml
from datetime import datetime

# Load settings
with open("mtr_script_settings.yaml") as f:
    settings = yaml.safe_load(f)

LOG_DIR = settings.get("log_directory", "logs")
HTML_DIR = "html"

# Load targets
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

# Generate index.html
index_path = os.path.join(HTML_DIR, "index.html")
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
        th { background-color: #f0f0f0; }
        tr:hover { background-color: #f5f5f5; }
        td a { text-decoration: none; color: #007bff; }
        td a:hover { text-decoration: underline; }
        .footer { margin-top: 20px; font-size: 0.9em; color: #666; }
    </style>
</head>
<body>
<h2>Monitored Targets</h2>
<table>
<tr><th>IP</th><th>Description</th><th>Status</th><th>Last Seen</th></tr>
""")

    for t in targets:
        ip = t["ip"]
        description = t.get("description", "")
        log_path = os.path.join(LOG_DIR, f"{ip}.log")

        status = "N/A"
        last_seen = "Never"
        if os.path.exists(log_path):
            with open(log_path) as logf:
                lines = [line.strip() for line in logf if "MTR RUN" in line]
                if lines:
                    last_line = lines[-1]
                    last_seen = last_line.split("]")[0].strip("[")
                    status = "Reachable"

        f.write("<tr>")
        f.write(f"<td><a href='{ip}.html'>{ip}</a></td>")
        f.write(f"<td>{description}</td>")
        f.write(f"<td>{status}</td>")
        f.write(f"<td>{last_seen}</td>")
        f.write("</tr>\n")

    f.write(f"""</table>
<div class='footer'>Generated at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
</body>
</html>""")

print("[UPDATED with styling] index.html")
