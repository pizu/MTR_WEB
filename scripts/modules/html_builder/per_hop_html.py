#!/usr/bin/env python3
# modules/html_builder/per_hop_html.py

import os
from modules.utils import load_settings, setup_logger

def generate_per_hop_html(ip, hops, description, settings):
    """
    Generates a separate HTML page (e.g. 1.1.1.1_hops.html) showing all graphs per hop.

    Each hop gets a grid of PNG images per metric (avg, loss, etc.) and time range.

    Parameters:
        ip (str): Target IP address
        hops (list[int]): List of hop numbers
        description (str): Optional description for the target

    Output:
        Saves: html/<ip>_hops.html
    """
    from modules.utils import setup_logger  # safe to import here if needed
    logger = setup_logger("per_hop_html", settings.get("log_directory", "/tmp"), "per_hop_html.log", settings=settings)
    
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
</head><body>
""")

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

                # Time Range Dropdown
                f.write(f"<label>Time Range: </label><select onchange=\"setHopTimeRange('{ip}', {hop}, this.value)\">")
                for i, tr in enumerate(TIME_RANGES):
                    label = tr['label']
                    selected = "selected" if i == 0 else ""
                    f.write(f"<option value='{label}' {selected}>{label.upper()}</option>")
                f.write("</select>")

                # Graphs for each metric
                for metric in ["avg", "last", "best", "loss"]:
                    f.write(f"<div class='graph-grid hop-metric-{safe_ip}-{hop}'>")
                    for i, tr in enumerate(TIME_RANGES):
                        label = tr['label']
                        png = f"{ip}_hop{hop}_{metric}_{label}.png"
                        if os.path.exists(os.path.join(GRAPH_DIR, png)):
                            display = "block" if i == 0 else "none"
                            f.write(f"<div style='display:{display}' class='hop-graph-{safe_ip}-{hop}' data-range='{label}'>")
                            f.write(f"<img src='graphs/{png}' alt='Hop {hop} {metric} {label}' loading='lazy'>")
                            f.write("</div>")
                    f.write("</div>")  # End of .graph-grid
                f.write("</div>")  # End of .graph-section

            f.write("</body></html>")
        logger.info(f"[{ip}] Per-hop HTML generated: {html_path}")

    except Exception as e:
        logger.exception(f"[{ip}] Failed to generate per-hop HTML")
