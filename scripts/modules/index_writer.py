#!/usr/bin/env python3
"""
modules/index_writer.py
-----------------------

Goal
====
1) Build a modern index.html (left sidebar + card grid) that **uses ranges
   from mtr_script_settings.yaml**, not hard-coded labels.
2) Add a simple **Settings** page (settings.html) to *view/edit* both
   mtr_script_settings.yaml and mtr_targets.yaml directly in the browser and
   **download** the modified YAML. (Static-only; no backend service.)
3) Add a **Light/Dark mode toggle** in the index header (persists in localStorage).

Important
---------
- We only read settings from mtr_script_settings.yaml and targets from
  mtr_targets.yaml (loaded by index_generator.py).
- We do NOT run servers or write files from the browser. The settings editor
  simply allows you to edit the YAML and download it; you then replace on disk.
- No PNGs; this page is ready for future JSON/Chart.js sparklines.

Safe split points
-----------------
You can split this module later into smaller files:
  - index_html_writer.py      # [split] write_index_html()
  - settings_html_writer.py   # [split] write_settings_html()
  - index_helpers.py          # [split] small helpers (escape, read_last_seen, etc.)

"""

import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from modules.fping_status import get_fping_status
from modules.utils import (
    resolve_html_dir,
    resolve_all_paths,
    get_html_ranges,   # <-- uses your centralized YAML ranges
)


# --------------------------
# Small helpers (split-ready)
# --------------------------

def _escape(s: Any) -> str:
    """Minimal HTML escaping for safe insertion in attributes/innerHTML."""
    if s is None:
        return ""
    s = str(s)
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


def _read_last_seen_from_log(log_path: str) -> str:
    """
    Extract a human-readable "Last Seen" timestamp.
    Priority:
      1) Last line containing 'MTR RUN' (prefix timestamp if present)
      2) File modification time
      3) 'Never' / 'Unknown'
    """
    if not os.path.exists(log_path):
        return "Never"

    last_line = None
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "MTR RUN" in line:
                    last_line = line.strip()
    except Exception:
        last_line = None

    if last_line:
        parts = last_line.split(" [", 1)
        ts = parts[0].strip() if parts else ""
        # Rough YYYY-MM-DD HH:MM:SS check
        if len(ts) >= 19 and ts[4] == "-" and ts[7] == "-" and ts[10] == " ":
            return ts
        return last_line

    try:
        return datetime.fromtimestamp(os.path.getmtime(log_path)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"


def _read_hop_count(traceroute_dir: str, ip: str) -> Optional[int]:
    """
    Return number of hop records from <traceroute>/<ip>_hops.json if present.
    """
    path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f) or []
            return len(arr)
    except Exception:
        pass
    return None


def _classify_status(raw: str) -> str:
    """
    Normalize fping output to: 'up' | 'down' | 'warn' | 'unknown'.
    (Currently maps 'alive' → up, 'unreachable' → down; extend as needed.)
    """
    if not raw:
        return "unknown"
    r = raw.strip().lower()
    if r == "alive":
        return "up"
    if r == "unreachable":
        return "down"
    return "unknown"


# -------------------------------
# Main entrypoints (split-ready)
# -------------------------------

def generate_index_page(targets: List[Dict[str, Any]], settings: Dict[str, Any], logger) -> None:
    """
    Public API used by index_generator.py
    - Writes index.html using ranges from settings.
    - Also writes a static settings.html editor page.
    """
    HTML_DIR = resolve_html_dir(settings)
    paths    = resolve_all_paths(settings)

    LOG_DIR    = paths["logs"]
    TRACE_DIR  = paths["traceroute"]
    FPING_PATH = paths.get("fping")

    ENABLE_FPING = settings.get("index_page", {}).get(
        "enable_fping_check",
        settings.get("enable_fping_check", True)
    )
    REFRESH_SECONDS = settings.get("html", {}).get(
        "auto_refresh_seconds",
        settings.get("html_auto_refresh_seconds", 0)
    )

    # Pull the actual ranges from your YAML using the same helper the target page uses.
    # Each item is a dict like: { label: "15m", seconds: 900, step: 60, ... }
    ranges_cfg = get_html_ranges(settings) or []
    RANGE_LABELS = [r.get("label") for r in ranges_cfg if r.get("label")] or ["15m"]

    # Cosmetics: default label shown on index header (first configured range).
    INDEX_RANGE_LABEL = RANGE_LABELS[0]

    os.makedirs(HTML_DIR, exist_ok=True)

    # Build data for cards
    cards = []
    for t in targets:
        ip = (t or {}).get("ip") or ""
        if not ip:
            continue
        desc = (t or {}).get("description", "") or ""
        log_path = os.path.join(LOG_DIR, f"{ip}.log")
        last_seen = _read_last_seen_from_log(log_path)

        status_raw = "Unknown"
        if ENABLE_FPING:
            try:
                status_raw = get_fping_status(ip, FPING_PATH)
            except Exception as e:
                logger.warning(f"fping status failed for {ip}: {e}")

        status_class = _classify_status(status_raw)
        hop_count = _read_hop_count(TRACE_DIR, ip)
        hop_text  = str(hop_count) if hop_count is not None else "—"

        cards.append({
            "ip": ip,
            "desc": desc,
            "status_class": status_class,
            "status_label": (status_raw or "Unknown").upper(),
            "last_seen": last_seen,
            "hops": hop_text,
        })

    # Write index.html
    write_index_html(
        html_dir=HTML_DIR,
        cards=cards,
        range_labels=RANGE_LABELS,
        default_range_label=INDEX_RANGE_LABEL,
        auto_refresh_seconds=int(REFRESH_SECONDS or 0),
        logger=logger
    )

    # Also write settings.html (static editor for YAML files)
    write_settings_html(
        html_dir=HTML_DIR,
        paths=paths,
        settings_path=settings.get("_loaded_from") or "mtr_script_settings.yaml",  # utils.load_settings often sets this; fallback is fine
        targets_path=paths.get("targets", "mtr_targets.yaml"),
        logger=logger
    )


def write_index_html(
    html_dir: str,
    cards: List[Dict[str, str]],
    range_labels: List[str],
    default_range_label: str,
    auto_refresh_seconds: int,
    logger
) -> None:
    """
    [split] Responsible only for writing index.html
    """
    index_path = os.path.join(html_dir, "index.html")
    chips_html = "\n".join(
        f"<div class='chip' data-range='{_escape(lbl)}'>{_escape(lbl)}</div>"
        for lbl in range_labels
    )

    try:
        with open(index_path, "w", encoding="utf-8") as f:
            f.write("<!doctype html><html lang='en'><head><meta charset='utf-8'>")
            if auto_refresh_seconds > 0:
                f.write(f"<meta http-equiv='refresh' content='{auto_refresh_seconds}'>")
                logger.info(f"[index] Auto-refresh enabled: {auto_refresh_seconds}s")
            else:
                logger.info("[index] Auto-refresh disabled")

            # Shared CSS variables for Dark/Light themes (toggle at runtime)
            f.write(f"""
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MTR • Dashboard</title>
<style>
  :root {{
    --bg:#0f1420; --panel:#131a28; --panel-2:#0d1320; --text:#e9eef7; --muted:#9fb0c6;
    --ok:#1faa70; --warn:#d7a021; --down:#cf3b43; --unknown:#6b7280;
    --outline:#26324a; --chip:#1b2538; --radius:14px;
  }}
  :root[data-theme="light"] {{
    --bg:#f6f7fb; --panel:#ffffff; --panel-2:#f2f4f9; --text:#10182a; --muted:#4b5563;
    --ok:#158f60; --warn:#9b750f; --down:#b1353c; --unknown:#6b7280;
    --outline:#d5d8e1; --chip:#eef2f7;
  }}

  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,Segoe UI,Roboto,Arial,sans-serif}}
  a{{color:inherit;text-decoration:none}}
  .layout{{display:grid;grid-template-columns:280px 1fr;min-height:100vh}}
  .sidebar{{
    background:linear-gradient(180deg,var(--panel),var(--panel-2));
    border-right:1px solid var(--outline);padding:16px;position:sticky;top:0;height:100vh;overflow:auto;
  }}
  .brand{{font-weight:700;font-size:18px;margin-bottom:10px}}
  .subtitle{{color:var(--muted);font-size:12px;margin-bottom:16px}}
  .section{{margin:14px 0;padding:12px;border:1px solid var(--outline);border-radius:var(--radius);background:var(--panel)}}
  .section h4{{margin:0 0 8px 0;font-size:12px;letter-spacing:.06em;color:var(--muted);text-transform:uppercase}}
  .search{{display:flex;gap:8px}}
  .search input{{
    width:100%;padding:10px;border-radius:10px;border:1px solid var(--outline);
    background:var(--panel-2);color:var(--text)
  }}
  .chips{{display:flex;flex-wrap:wrap;gap:8px}}
  .chip{{
    padding:6px 10px;border-radius:999px;background:var(--chip);border:1px solid var(--outline);
    font-size:12px;cursor:pointer;user-select:none
  }}
  .chip.ok{{border-color:var(--ok);color:var(--ok)}}
  .chip.warn{{border-color:var(--warn);color:var(--warn)}}
  .chip.down{{border-color:var(--down);color:var(--down)}}
  .chip.unknown{{border-color:var(--unknown);color:var(--unknown)}}
  .nav a{{display:block;padding:10px;border-radius:10px;color:var(--text);opacity:.9}}
  .nav a:hover{{background:var(--panel-2)}}
  .content{{padding:20px}}
  .header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px;flex-wrap:wrap}}
  .header h1{{font-size:18px;margin:0}}
  .header .right{{display:flex;align-items:center;gap:10px}}
  .theme-toggle{{border:1px solid var(--outline);background:var(--panel-2);padding:6px 10px;border-radius:10px;cursor:pointer}}
  .grid{{
    display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
    gap:14px
  }}
  .card{{
    border:1px solid var(--outline);border-radius:var(--radius);background:var(--panel);padding:14px
  }}
  .card-top{{display:flex;justify-content:space-between;gap:10px;align-items:center}}
  .ip{{font-weight:700}}
  .status{{font-size:12px;padding:4px 8px;border-radius:999px;border:1px solid var(--outline)}}
  .status.ok{{border-color:var(--ok);color:var(--ok)}}
  .status.warn{{border-color:var(--warn);color:var(--warn)}}
  .status.down{{border-color:var(--down);color:var(--down)}}
  .status.unknown{{border-color:var(--unknown);color:var(--unknown)}}
  .desc{{color:var(--muted);margin:6px 0 10px 0}}
  .meta{{color:var(--muted);font-size:12px;margin-bottom:10px}}
  .actions{{display:flex;gap:8px;flex-wrap:wrap}}
  .btn{{padding:8px 10px;border-radius:10px;background:var(--panel-2);border:1px solid var(--outline);font-size:13px}}
  .btn:hover{{filter:brightness(1.05)}}
  .spark{{height:34px;border-radius:8px;background:var(--panel-2);border:1px dashed var(--outline);
    display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:12px;margin-bottom:10px}}
  .footer{{color:var(--muted);font-size:12px;margin-top:12px}}
</style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <div class="brand">MTR • Dashboard</div>
    <div class="subtitle">Overview and quick controls</div>

    <div class="section">
      <h4>Search</h4>
      <div class="search"><input id="q" type="search" placeholder="Search IP or description"></div>
    </div>

    <div class="section">
      <h4>Time Range</h4>
      <div class="chips">
        {chips}
      </div>
    </div>

    <div class="section">
      <h4>Status</h4>
      <div class="chips">
        <div class="chip ok" data-status="up">Up</div>
        <div class="chip warn" data-status="warn">Warn</div>
        <div class="chip down" data-status="down">Down</div>
        <div class="chip unknown" data-status="unknown">Unknown</div>
      </div>
    </div>

    <div class="section nav">
      <h4>Navigation</h4>
      <a href="index.html">Index</a>
      <a href="settings.html">Settings</a>
      <a href="logs/">Logs folder</a>
    </div>
  </aside>

  <main class="content">
    <div class="header">
      <h1>Targets Overview</h1>
      <div class="right">
        <div class="subtitle">Showing: <strong id="count">0</strong> • Range: <strong id="rangeLabel">{default_range}</strong></div>
        <button id="themeBtn" class="theme-toggle" title="Toggle Light/Dark">🌓 Theme</button>
      </div>
    </div>

    <div class="grid" id="cards">
""".format(
                chips=chips_html,
                default_range=_escape(default_range_label),
            ))

            # Cards
            for c in cards:
                ip   = _escape(c["ip"])
                desc = _escape(c["desc"])
                status_class = c["status_class"]
                status_label = _escape(c["status_label"])
                last_seen = _escape(c["last_seen"])
                hops = _escape(c["hops"])

                f.write(
                    "      <div class='card' data-ip='{ip}' data-status='{status}'>\n"
                    "        <div class='card-top'>\n"
                    "          <div class='ip'>{ip}</div>\n"
                    "          <div class='status {status}' title='{label}'>{label}</div>\n"
                    "        </div>\n"
                    "        <div class='desc'>{desc}</div>\n"
                    "        <div class='meta'>Last seen: {last} • Hops: {hops} • Loss: —</div>\n"
                    "        <div class='spark' id='spark-{ip}'>[mini trend]</div>\n"
                    "        <div class='actions'>\n"
                    "          <a class='btn' href='{ip}.html'>View Details</a>\n"
                    "          <a class='btn' href='logs/{ip}.log'>Logs</a>\n"
                    "        </div>\n"
                    "      </div>\n".format(
                        ip=ip, status=status_class, label=status_label,
                        desc=desc, last=last_seen, hops=hops
                    )
                )

            # Footer + JS
            f.write("""
    </div>
    <div class="footer">
      Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """ — Auto-refresh: """ + ("enabled" if auto_refresh_seconds > 0 else "disabled") + """
    </div>
  </main>
</div>

<script>
  // --- Theme toggle (persist to localStorage) ---
  (function initTheme(){
    const saved = localStorage.getItem('mtr_theme') || 'dark';
    if (saved === 'light') document.documentElement.setAttribute('data-theme','light');
    document.getElementById('themeBtn').addEventListener('click', () => {
      const cur = document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
      const next = (cur === 'light') ? 'dark' : 'light';
      if (next === 'light') document.documentElement.setAttribute('data-theme','light');
      else document.documentElement.removeAttribute('data-theme');
      localStorage.setItem('mtr_theme', next);
    });
  })();

  // --- Search + Status filters ---
  const q = document.getElementById('q');
  const cards = document.getElementById('cards');
  const rangeLabel = document.getElementById('rangeLabel');
  const countEl = document.getElementById('count');

  function updateVisibleCount(){
    const visible = Array.from(cards.children).filter(el => el.style.display !== 'none').length;
    countEl.textContent = String(visible);
  }

  q.addEventListener('input', () => {
    const term = q.value.toLowerCase();
    Array.from(cards.children).forEach(c => {
      const ip = (c.dataset.ip || '').toLowerCase();
      const desc = (c.querySelector('.desc')?.textContent || '').toLowerCase();
      c.style.display = (ip.includes(term) || desc.includes(term)) ? '' : 'none';
    });
    updateVisibleCount();
  });

  document.querySelectorAll('.chip[data-status]').forEach(chip => {
    chip.addEventListener('click', () => {
      const s = chip.dataset.status;
      const active = chip.classList.toggle('active');
      document.querySelectorAll('.chip[data-status]').forEach(c => { if (c!==chip) c.classList.remove('active'); });
      Array.from(cards.children).forEach(c => {
        c.style.display = (!active || c.dataset.status === s) ? '' : 'none';
      });
      updateVisibleCount();
    });
  });

  document.querySelectorAll('.chip[data-range]').forEach(chip => {
    chip.addEventListener('click', () => {
      rangeLabel.textContent = chip.dataset.range;
      // Future: trigger sparkline reload for this range.
    });
  });

  // Initial count
  updateVisibleCount();
</script>

</body></html>
""")
        logger.info(f"[index] Wrote {index_path} with {len(cards)} targets")
    except Exception as e:
        logger.error(f"[index] Failed to generate index.html: {e}")


def write_settings_html(
    html_dir: str,
    paths: Dict[str, str],
    settings_path: str,
    targets_path: str,
    logger
) -> None:
    """
    [split] Write a static settings.html so operators can:
      - View current YAML (settings + targets) in big textareas
      - Edit them in the browser
      - Click "Download" to save the edited YAML locally
    Deployment: upload the downloaded file to the server to replace originals.
    """
    page = os.path.join(html_dir, "settings.html")

    # Read current YAML files into strings (best-effort; we don't parse here).
    settings_text = _read_text_safely(settings_path)
    targets_text  = _read_text_safely(targets_path)

    try:
        with open(page, "w", encoding="utf-8") as f:
            f.write(f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MTR • Settings Editor</title>
<style>
  body{{margin:0;font:14px/1.45 system-ui,Segoe UI,Roboto,Arial,sans-serif;background:#0f1420;color:#e9eef7}}
  .wrap{{max-width:1200px;margin:24px auto;padding:0 16px}}
  .card{{background:#131a28;border:1px solid #26324a;border-radius:14px;padding:16px;margin-bottom:16px}}
  h1{{margin:8px 0 16px 0;font-size:20px}}
  h2{{margin:8px 0 8px 0;font-size:16px;color:#9fb0c6}}
  .row{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
  textarea{{width:100%;min-height:420px;background:#0d1320;color:#e9eef7;border:1px solid #26324a;border-radius:10px;padding:10px;font-family:ui-monospace,Consolas,Menlo,monospace}}
  .btns{{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap}}
  button, a.btn{{border:1px solid #26324a;background:#0d1320;color:#e9eef7;border-radius:10px;padding:8px 10px;cursor:pointer;text-decoration:none}}
  .note{{color:#9fb0c6;font-size:12px;margin-top:6px}}
  .hdr{{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}}
  .link{{color:#93c5fd;text-decoration:none}}
</style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <h1>MTR • Settings Editor</h1>
    <div>
      <a class="link" href="index.html">← Back to Index</a>
    </div>
  </div>

  <div class="card">
    <h2>Instructions</h2>
    <ol>
      <li>Edit the YAML below.</li>
      <li>Click <strong>Download</strong> to save the file locally.</li>
      <li>Upload/replace on the server:
        <ul>
          <li><code>{_escape(settings_path)}</code> for script settings</li>
          <li><code>{_escape(targets_path)}</code> for targets</li>
        </ul>
      </li>
      <li>Re-run your pipeline or let the controller regenerate pages.</li>
    </ol>
    <p class="note">This editor is static (no server). It cannot write to your filesystem directly.</p>
  </div>

  <div class="row">
    <div class="card">
      <h2>mtr_script_settings.yaml</h2>
      <textarea id="settingsTa">{_escape(settings_text)}</textarea>
      <div class="btns">
        <button onclick="downloadYaml('mtr_script_settings.yaml', document.getElementById('settingsTa').value)">Download settings.yaml</button>
      </div>
      <p class="note">Tip: ranges shown on index are read from <code>html.ranges</code> (via <code>get_html_ranges()</code>).</p>
    </div>

    <div class="card">
      <h2>mtr_targets.yaml</h2>
      <textarea id="targetsTa">{_escape(targets_text)}</textarea>
      <div class="btns">
        <button onclick="downloadYaml('mtr_targets.yaml', document.getElementById('targetsTa').value)">Download targets.yaml</button>
      </div>
      <p class="note">Each target supports <code>ip</code> and optional <code>description</code> fields.</p>
    </div>
  </div>
</div>

<script>
function downloadYaml(filename, text) {{
  const blob = new Blob([text], {{ type: 'text/yaml' }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  setTimeout(() => {{ URL.revokeObjectURL(url); a.remove(); }}, 0);
}}
</script>
</body>
</html>""")
        logger.info(f"[index] Wrote settings editor {page}")
    except Exception as e:
        logger.error(f"[index] Failed to write settings.html: {e}")


def _read_text_safely(path: str) -> str:
    """Read a text file best-effort; return empty string if missing/error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""
