#!/usr/bin/env python3
# modules/html_builder/target_html.py
#
# Interactive Chart.js renderer.
#
# Update (per-hop 'varies' flag support):
# - Colors remain by hop index (stable).
# - Legend chips show a "varies" badge when a hop's endpoint changed across exports.
# - Tooltip title includes a note when varies=true.

import os, re, html
from datetime import datetime
from modules.utils import (
    setup_logger,
    resolve_html_dir,
    resolve_all_paths,
    resolve_html_knobs,
    get_html_ranges,
)

# Metric labels (unchanged; we do NOT add a 'varies' metric)
METRIC_LABELS = {
    "avg": "Avg (ms)",
    "last": "Last (ms)",
    "best": "Best (ms)",
    "loss": "Loss (%)",
}

def generate_target_html(ip, description, hops, settings, logger=None):
    logger = logger or setup_logger("target_html", settings=settings)

    # Unified paths
    paths     = resolve_all_paths(settings)
    HTML_DIR  = resolve_html_dir(settings)               # ensures exists
    DATA_DIR  = os.path.join(HTML_DIR, "data")
    LOG_DIR   = paths["logs"]
    TRACE_DIR = paths["traceroute"]
    RRD_DIR   = paths["rrd"]

    os.makedirs(DATA_DIR, exist_ok=True)

    # HTML knobs
    REFRESH_SECONDS, LOG_LINES_DISPLAY = resolve_html_knobs(settings)

    # Time ranges (fallback to legacy graph_time_ranges inside helper)
    TIME_RANGES = [r for r in (get_html_ranges(settings) or []) if r.get("label")]

    # Metrics come from DS names in settings (no 'varies' here)
    schema_metrics = [ds["name"] for ds in settings.get("rrd", {}).get("data_sources", []) if ds.get("name")]
    METRICS = [m for m in schema_metrics if m in METRIC_LABELS]

    html_path  = os.path.join(HTML_DIR, f"{ip}.html")
    log_path   = os.path.join(LOG_DIR, f"{ip}.log")
    trace_path = os.path.join(TRACE_DIR, f"{ip}.trace.txt")

    # Tail logs
    logs = []
    if os.path.exists(log_path):
        try:
            with open(log_path, encoding="utf-8") as f:
                logs = [line.rstrip("\n") for line in f if line.strip()]
                logs = logs[-LOG_LINES_DISPLAY:][::-1]
        except Exception as e:
            logger.warning(f"Could not read logs for {ip}: {e}")

    # Read traceroute (single snapshot for table)
    traceroute = []
    if os.path.exists(trace_path):
        try:
            with open(trace_path, encoding="utf-8") as f:
                traceroute = f.read().splitlines()
        except Exception as e:
            logger.warning(f"Could not read traceroute for {ip}: {e}")

    # HTML
    os.makedirs(HTML_DIR, exist_ok=True)
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write("<!doctype html><html><head><meta charset='utf-8'>")
            if REFRESH_SECONDS > 0:
                f.write(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>")
            f.write(f"<title>{ip}</title>")
            f.write("""
<style>
:root { --bg:#0f172a; --panel:#111827; --muted:#94a3b8; --text:#e5e7eb; --border:#1f2937; --chip:#0b1220; --accent:#fde68a; }
body { margin:0; background:var(--bg); color:var(--text); font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial; }
.wrap{ max-width:1100px; margin:32px auto; padding:0 16px; }
.card{ background:var(--panel); border:1px solid var(--border); border-radius:16px; overflow:hidden; box-shadow:0 10px 30px rgba(0,0,0,.25); }
.card header{ padding:16px 20px; border-bottom:1px solid var(--border); }
.card header h1{ font-size:18px; margin:0 0 4px; }
.card header p{ margin:0; color:var(--muted); }
.toolbar{ display:flex; gap:12px; align-items:center; justify-content:space-between; padding:12px 20px; border-bottom:1px solid var(--border); flex-wrap:wrap; }
.legend{ display:flex; flex-wrap:wrap; gap:10px; }
.legend .item { display:flex; align-items:center; gap:8px; padding:6px 10px; background:var(--chip); border:1px solid var(--border); border-radius:999px; cursor:pointer; user-select:none; color: var(--text); }
.legend .item.dim{ opacity:.35; }
.legend .swatch{ width:12px; height:12px; border-radius:3px; border:1px solid #00000055; }
.legend .badge { font-size:10px; padding:2px 6px; border-radius:999px; border:1px dashed #00000055; background:var(--accent); color: var(--text); }
.panel{ padding:16px 20px; }
.note{ color:var(--muted); font-size:12px; margin-top:8px; }
select{ background:#0b1220; color:#e5e7eb; border:1px solid var(--border); border-radius:8px; padding:6px 10px; }
.chart-container{ width:100%; height:420px; }
canvas{ width:100% !important; height:100% !important; }
h3{ margin:18px 0 8px; }
table { border-collapse: collapse; width:100%; }
th, td { border: 1px solid #334155; padding: 6px 8px; text-align: left; }
.log-line { white-space: pre-wrap; }
.log-table pre { margin: 0; max-height: 140px; overflow:auto; background-color:#0b1220; padding:4px; border-radius: 4px; font-family: monospace; }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
</head>
<body>
<div class="wrap">
  <div class="card">
    <header>
      <h1>Interactive MTR Graph — """ + html.escape(ip) + """</h1>
      <p>Hover for tooltips; click legend chips to toggle; Alt+click to solo.</p>
    </header>
    <div class="toolbar">
      <div><label for="metric">Metric:</label> <select id="metric"></select></div>
      <div><label for="range">Time Range:</label> <select id="range"></select></div>
      <div class="note">Colors are stable per hop number; "varies" highlights hops whose endpoint changed.</div>
    </div>
    <div class="panel">
      <div class="chart-container"><canvas id="mtrChart"></canvas></div>
      <div id="legend" class="legend" aria-label="Hop legend"></div>
      <div class="note"></div>
    </div>
  </div>

  <h3>Traceroute</h3>
  <table><tr><th>Hop</th><th>Address</th><th>Details</th></tr>""")

            # Basic traceroute table
            for idx, line in enumerate(traceroute, start=1):
                parts = line.strip().split()
                hop_ip  = parts[1] if len(parts) >= 2 else "???"
                latency = parts[2] + " " + parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else "-")
                if hop_ip in ("???", "Request", "request") or hop_ip.lower().startswith("request"):
                    hop_ip, latency = "Request timed out", "-"
                f.write(f"<tr><td>{idx}</td><td>{html.escape(hop_ip)}</td><td>{html.escape(latency)}</td></tr>")
            f.write("</table>")

            # Logs
            f.write("""
  <h3>Recent Logs</h3>
  <input type="text" id="logFilter" placeholder="Filter logs..." style="width:100%;margin-bottom:10px;padding:5px;">
  <table class="log-table"><thead><tr><th>Timestamp</th><th>Level</th><th>Message</th></tr></thead><tbody>""")
            log_line_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[(\w+)\] (.*)")
            for line in logs:
                m = log_line_re.match(line)
                ts, level, msg = m.groups() if m else ("", "", line)
                color = {"DEBUG":"#94a3b8","INFO":"#86efac","WARNING":"#fbbf24","ERROR":"#f87171"}.get((level or "").upper(),"#e5e7eb")
                f.write(f"<tr class='log-line'><td>{ts}</td><td style='color:{color}'>{html.escape(level)}</td><td><pre>{html.escape(msg)}</pre></td></tr>")
            f.write("""</tbody></table>

  <p class="note">Generated: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """ — """ + ("Auto-refresh enabled" if REFRESH_SECONDS > 0 else "Auto-refresh disabled") + """</p>
  <p><a href="index.html" style="color:#93c5fd">Back to index</a></p>
</div>

<script>
// Metrics (no 'varies' here — it's a hop property, not a metric)
const METRICS = """ + _json_array(METRICS) + """;
const RANGES  = """ + _json_array([r["label"] for r in TIME_RANGES]) + """;
const DATA_DIR = "data";
const IP = """ + _json_quote(ip) + """;
const LABELS = """ + _labels_json(METRICS) + """;

const metricSel = document.getElementById('metric');
const rangeSel  = document.getElementById('range');
const legendEl  = document.getElementById('legend');
const ctx = document.getElementById('mtrChart').getContext('2d');

function fillSelectWithLabels(sel, keys, labelsMap) {
  sel.innerHTML = '';
  keys.forEach((k) => {
    const opt = document.createElement('option');
    opt.value = k;
    opt.textContent = labelsMap[k] || (k || '').toUpperCase();
    sel.appendChild(opt);
  });
  sel.selectedIndex = 0;
}

function buildDatasetsFromBundle(bundle, metric) {
  return (bundle.hops || []).map(h => ({
    label: h.name + (h.varies ? " (varies)" : ""),
    data: (h.metrics && h.metrics[metric]) ? h.metrics[metric] : [],
    borderColor: h.color || '#888',
    backgroundColor: h.color || '#888',
    spanGaps: true,
    borderWidth: 2,
    pointRadius: 2,
    tension: 0.25,
    yAxisID: metric === 'loss' ? 'yLoss' : 'yLatency'
  }));
}

let currentBundle = null;
let currentMetric = null;
let chart = new Chart(ctx, {
  type: 'line',
  data: { labels: [], datasets: [] },
  options: {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'nearest', intersect: false },
    plugins: {
      tooltip: {
        callbacks: {
          title: (items) => {
            const it = items[0];
            if (!it) return '';
            const ds = it.chart.data.datasets[it.datasetIndex];
            return ds ? ds.label : '';
          },
          label: (item) => {
            const val = item.parsed.y;
            const unit = currentMetric === 'loss' ? '%' : ' ms';
            return `${val}${unit}`;
          }
        }
      },
      legend: { display: false }
    },
    scales: {
      x: { grid: { color: '#1f2937' }, ticks: { color: '#cbd5e1' } },
      yLatency: { type: 'linear', position: 'left', grid: { color: '#1f2937' }, ticks: { color: '#cbd5e1' }, title: { display: true, text: 'Latency (ms)' } },
      yLoss: { type: 'linear', position: 'right', grid: { drawOnChartArea: false }, ticks: { color: '#cbd5e1', callback: (v)=> v + '%' }, title: { display: true, text: 'Loss (%)' }, min: 0, max: 100 }
    }
  }
});

function renderLegend() {
  legendEl.innerHTML = '';
  chart.data.datasets.forEach((ds, idx) => {
    const item = document.createElement('button');
    item.className = 'item' + (ds.hidden ? ' dim' : '');
    item.title = (ds.label || '');
    item.onclick = (ev) => {
      if (ev.altKey) {
        const visible = chart.data.datasets.filter(d=>!d.hidden);
        const soloHidden = (visible.length === 1 && !ds.hidden);
        chart.data.datasets.forEach((d,i)=> d.hidden = soloHidden ? false : (i !== idx));
      } else {
        ds.hidden = !ds.hidden;
      }
      chart.update(); renderLegend();
    };
    const swatch = document.createElement('span'); swatch.className = 'swatch';
    // Extract the borderColor to render the chip color
    swatch.style.backgroundColor = ds.borderColor || '#888';

    const label = document.createElement('span'); 
    label.textContent = ds.label.replace(/\s*\(varies\)\s*$/, "");

    item.appendChild(swatch); 
    item.appendChild(label);

    // If the dataset label indicates varies, add a badge
    if (/\(varies\)\s*$/.test(ds.label)) {
      const badge = document.createElement('span'); 
      badge.className = 'badge'; 
      badge.textContent = 'varies';
      item.appendChild(badge);
    }

    legendEl.appendChild(item);
  });
}

function fmtTime(epoch) { const d = new Date(epoch * 1000); return d.toLocaleString(); }

async function loadBundle(rangeLabel) {
  const url = `${DATA_DIR}/${IP}_${rangeLabel}.json?t=${Date.now()}`;
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`fetch failed: ${url}`);
  currentBundle = await res.json();

  chart.data.labels = currentBundle.timestamps || [];

  const last = (currentBundle.epoch && currentBundle.epoch.length)
    ? currentBundle.epoch[currentBundle.epoch.length - 1] : null;
  const noteEl = document.querySelector('.note');
  if (noteEl && last) noteEl.textContent = `Last updated: ${fmtTime(last)} (range: ${currentBundle.label})`;

  const wanted = currentMetric && METRICS.includes(currentMetric) ? currentMetric : (METRICS[0] || 'avg');
  setMetric(wanted);
}

function setMetric(metric) {
  currentMetric = metric;
  chart.data.datasets = buildDatasetsFromBundle(currentBundle || {}, metric);
  chart.update(); renderLegend();
}

function onMetricChange() { setMetric(metricSel.value); }
async function onRangeChange() { await loadBundle(rangeSel.value); }

function filterLogs() {
  const input = document.getElementById('logFilter').value.toLowerCase();
  const lines = document.getElementsByClassName('log-line');
  for (const line of lines) line.style.display = line.innerText.toLowerCase().includes(input) ? '' : 'none';
}

function labelsFor(keys) {
  const m = {};
  for (const k of keys) {
    m[k] = (""" + _labels_dict_js() + """)[k] || (k || '').toUpperCase();
  }
  return m;
}

function _init() {
  const labelsMap = labelsFor(METRICS);
  fillSelectWithLabels(metricSel, METRICS, labelsMap);
  fillSelectWithLabels(rangeSel, RANGES, Object.fromEntries(RANGES.map(r=>[r,r])));
  metricSel.addEventListener('change', onMetricChange);
  rangeSel.addEventListener('change', onRangeChange);
  onRangeChange();
}
_init();
</script>
</body></html>""")
        logger.info(f"Generated interactive HTML for {ip}")
    except Exception:
        logger.exception(f"[{ip}] Failed to generate target HTML")

def _json_quote(s: str) -> str:
    return '"' + (s or "").replace('\\', '\\\\').replace('"', '\\"') + '"'

def _json_array(arr):
    out = []
    for v in (arr or []):
        if v is None: continue
        s = str(v).replace('\\', '\\\\').replace('"', '\\"')
        out.append('"' + s + '"')
    return "[" + ",".join(out) + "]"

def _labels_json(metric_keys):
    pairs = []
    for k in (metric_keys or []):
        label = METRIC_LABELS.get(k, (k or "").upper())
        pairs.append(_json_quote(k) + ":" + _json_quote(label))
    return "{" + ",".join(pairs) + "}"

def _labels_dict_js():
    parts = []
    for k, v in METRIC_LABELS.items():
        parts.append(f'"{k}":"{v}"')
    return "{" + ",".join(parts) + "}"
