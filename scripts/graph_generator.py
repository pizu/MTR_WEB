#!/usr/bin/env python3
import os
import yaml
from datetime import datetime
from utils import load_settings, setup_logger

# Load config
settings = load_settings()
log_directory = settings.get("log_directory", "/tmp")
logger = setup_logger("html_generator", log_directory, "html_generator.log")

LOG_DIR = log_directory
GRAPH_DIR = settings.get("graph_output_directory", "html/graphs")
HTML_DIR = "html"
TRACEROUTE_DIR = "traceroute"
LOG_LINES_DISPLAY = settings.get("log_lines_display", 50)

# Load targets
with open("mtr_targets.yaml") as f:
    targets = yaml.safe_load(f)["targets"]

# Load traceroute into list of (hop, ip/host, latency)
def load_traceroute(ip):
    path = os.path.join(TRACEROUTE_DIR, f"{ip}.trace.txt")
    if not os.path.exists(path):
        return []

    hops = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=2)
            hop_num = parts[0]
            hop_host = parts[1] if len(parts) > 1 else "(unknown)"
            latency = parts[2] if len(parts) > 2 else "(no latency)"
            hops.append((hop_num, hop_host, latency))
    return hops

# Load logs
def load_logs(ip):
    path = os.path.join(LOG_DIR, f"{ip}.log")
    if not os.path.exists(path):
        return ["No log file found."]
    with open(path) as f:
        lines = f.readlines()
        if not lines:
            return ["No log entries."]
        return lines[-LOG_LINES_DISPLAY:][::-1]

# Generate HTML per IP
def generate_html(ip, description):
    traceroute = load_traceroute(ip)
    logs = load_logs(ip)
    html_path = os.path.join(HTML_DIR, f"{ip}.html")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(html_path, "w") as f:
        f.write(f"""<html>
<head>
    <meta charset='utf-8'>
    <title>{ip} Monitoring</title>
    <style>
        body {{ font-family: Arial, sans-serif; background-color: #f9f9f9; margin: 20px; }}
        h2 {{ color: #2c3e50; }}
        .section {{ margin-bottom: 30px; }}
        pre {{ background-color: #f0f0f0; padding: 10px; overflow-x: auto; border: 1px solid #ccc; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ccc; padding: 8px; text-align: left; }}
        th {{ background-color: #eee; }}
    </style>
</head>
<body>
    <h2>{ip}</h2>
    <p><strong>Description:</strong> {description or 'N/A'}</p>
    <p><strong>Last updated:</strong> {timestamp}</p>

    <div class="section">
        <h3>Traceroute (Hop Table)</h3>
        <table>
            <tr><th>Hop</th><th>IP / Host</th><th>Latency</th></tr>
""")
        if traceroute:
            for hop_num, hop_host, latency in traceroute:
                f.write(f"<tr><td>{hop_num}</td><td>{hop_host}</td><td>{latency}</td></tr>\n")
        else:
            f.write("<tr><td colspan='3'>No traceroute data found.</td></tr>\n")

        f.write(f"""</table>
        <p><a href='../{TRACEROUTE_DIR}/{ip}.trace.txt' target='_blank'>View raw traceroute file</a></p>
    </div>

    <div class="section">
        <h3>Graphs</h3>
""")
        for metric in ["avg", "last", "best", "loss"]:
            img_path = os.path.join(GRAPH_DIR, f"{ip}_{metric}.png")
            if os.path.exists(img_path):
                f.write(f"<h4>{metric.upper()} Graph</h4><img src='../{img_path}' alt='{metric} graph'><br><br>\n")
            else:
                f.write(f"<p>{metric.upper()} graph not found.</p>\n")

        f.write("""</div>
    <div class="section">
        <h3>Recent Log Events</h3>
        <pre>
""")
        for line in logs:
            f.write(line)
        f.write("""</pre>
    </div>
</body>
</html>""")

    logger.info(f"[GENERATED] HTML for {ip} at {html_path}")

# Generate HTML for all targets
for target in targets:
    ip = target.get("ip")
    description = target.get("description", "")
    generate_html(ip, description)
