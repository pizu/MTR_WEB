# index_generator.py
#
# Generates an index.html file linking to each per-target HTML report with basic status info.

import os
import yaml
from datetime import datetime

def load_targets():
    with open("mtr_targets.yaml", "r") as f:
        return yaml.safe_load(f)['targets']

def parse_last_seen(log_path):
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
        for line in reversed(lines):
            if "Started monitoring" in line or "Packet loss" in line or "Hop change" in line:
                ts = line.split()[0] + " " + line.split()[1]
                return ts
        return "No recent activity"
    except Exception:
        return "No log"

def get_status(log_path):
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
        for line in reversed(lines):
            if "Packet loss" in line:
                return "⚠️ Loss Detected"
            if "Started monitoring" in line:
                return "✅ OK"
        return "❌ Unreachable"
    except Exception:
        return "❌ No Log"

def generate_index_html(targets, output_path, log_dir):
    with open(output_path, "w") as f:
        f.write("""<!DOCTYPE html>
<html>
<head>
    <title>MTR Monitoring Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; padding: 20px; background: #f4f4f4; }
        h1 { margin-bottom: 20px; }
        .card {
            background: white;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }
        .card h2 { margin: 0 0 10px 0; }
        .meta { font-size: 14px; color: #555; }
        a { color: #007BFF; text-decoration: none; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>MTR Monitoring Dashboard</h1>
""")
        for t in targets:
            ip = t['ip']
            log_path = os.path.join(log_dir, f"{ip}.log")
            status = get_status(log_path)
            last_seen = parse_last_seen(log_path)
            f.write(f"""<div class="card">
    <h2><a href="{ip}.html">{ip}</a></h2>
    <div class="meta">Status: {status}</div>
    <div class="meta">Last Seen: {last_seen}</div>
</div>
""")

        f.write("""</body>
</html>
""")

def main():
    targets = load_targets()
    config = yaml.safe_load(open("mtr_script_settings.yaml"))
    log_dir = config['log_directory']
    os.makedirs("html", exist_ok=True)
    generate_index_html(targets, "html/index.html", log_dir)

if __name__ == "__main__":
    main()
