#!/usr/bin/env python3
import os
import re
import rrdtool
from datetime import datetime
from utils import load_settings, setup_logger

# Global config and logger
settings = load_settings()
logger = setup_logger("html_builder", settings.get("log_directory", "/tmp"), "html_builder.log", settings=settings)

def get_hop_metrics_from_rrd(ip, hop):
    rrd_dir = settings.get("rrd_directory", "rrd")
    rrd_path = os.path.join(rrd_dir, f"{ip}.rrd")
    if not os.path.exists(rrd_path):
        return {}

    try:
        end = int(datetime.now().timestamp())
        start = end - 120
        (start_ts, end_ts, step), ds_names, rows = rrdtool.fetch(
            rrd_path, "AVERAGE", "--start", str(start), "--end", str(end)
        )
        if not rows:
            return {}

        latest = next((r for r in reversed(rows) if any(v is not None for v in r)), None)
        if not latest:
            return {}

        metrics = {}
        hop_prefix = f"hop{hop}_"
        for i, name in enumerate(ds_names):
            if name.startswith(hop_prefix):
                metric = name[len(hop_prefix):]
                val = latest[i]
                if val is not None:
                    metrics[metric] = round(val, 1)
        return metrics
    except Exception as e:
        logger.warning(f"[RRD] Failed to read metrics for {ip} hop {hop}: {e}")
        return {}

def generate_target_html(ip, description, hops):
    ...
    # (UNCHANGED - this remains the same as in your current version)
    ...
    generate_per_hop_html(ip, hops, description)

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
                
                # Inject plain-text metrics below header
                metrics = get_hop_metrics_from_rrd(ip, hop)
                if metrics:
                    metrics_str = ", ".join(f"{k}: {v}" for k, v in metrics.items())
                    f.write(f"<p style='margin:5px 0 10px 0;' title='From last RRD sample'>{metrics_str}</p>")

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
