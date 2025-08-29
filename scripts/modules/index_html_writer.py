#!/usr/bin/env python3
"""
modules/index_html_writer.py
============================

Writes the unified Dashboard (index.html) with:
- Left sidebar (Search, Status chips, Time ranges from YAML)
- Right card grid (per target)
- Top-right Light/Dark theme toggle
- Embedded Settings Drawer (edit YAMLs in-place; Download locally)

Safety
------
- Avoid f-strings in large HTML blocks to prevent brace parsing issues.
- Use .format(...) with {{ }} escaping for literal braces.
- Write to a temporary file first, then atomic replace.

Logging
-------
- INFO: start/end, file paths, counts
- DEBUG: range labels, defaults, paths
- WARN/ERROR: failures, fallbacks
"""

import os
from datetime import datetime
from typing import Dict, List
from modules.index_helpers import html_escape


def _read_text_safely(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def _atomic_write(path: str, content: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


def write_index_html(
    html_dir: str,
    cards: List[Dict[str, str]],
    range_labels: List[str],
    default_range_label: str,
    auto_refresh_seconds: int,
    settings_path: str,
    targets_path: str,
    logger
) -> None:
    """
    Writes <html_dir>/index.html with embedded Settings drawer.
    """
    os.makedirs(html_dir, exist_ok=True)
    index_path = os.path.join(html_dir, "index.html")
    logger.info(f"[index] Writing {index_path} …")

    # Sidebar chips (from YAML ranges)
    chips_html = "\n        ".join(
        "<div class='chip' data-range='{lbl}'>{lbl}</div>".format(lbl=html_escape(lbl))
        for lbl in (range_labels or [])
    )

    # Read current YAML texts for the Settings drawer
    settings_text = html_escape(_read_text_safely(settings_path))
    targets_text  = html_escape(_read_text_safely(targets_path))
    logger.debug(f"[index] Prefilled settings drawer from {settings_path} and {targets_path}")

    # Build cards HTML
    cards_html_parts = []
    for c in (cards or []):
        ip   = html_escape(c["ip"])
        desc = html_escape(c["desc"])
        status_class = c["status_class"]
        status_label = html_escape(c["status_label"])
        last_seen = html_escape(c["last_seen"])
        hops = html_escape(c["hops"])

        card = (
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
            "      </div>\n"
        ).format(ip=ip, status=status_class, label=status_label, desc=desc, last=last_seen, hops=hops)
        cards_html_parts.append(card)

    cards_html = "".join(cards_html_parts)

    # HEAD (meta refresh injected if >0)
    meta_refresh = ""
    if auto_refresh_seconds > 0:
        meta_refresh = "<meta http-equiv='refresh' content='{s}'>".format(s=int(auto_refresh_seconds))

    # Full page template (all literal braces are doubled)
    page = """<!doctype html>
<html lang='en'>
<head>
<meta charset='utf-8'>
{meta_refresh}
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MTR • Dashboard</title>
<style>
  :root {{
    --bg:#0f1420; --panel:#131a28; --panel-2:#0d1320; --text:#e9eef7; --muted:#9fb0c6;
    --ok:#1faa70; --warn:#d7a021; --down:#cf3b43; --unknown:#6b7280;
    --outline:#26324a; --chip:#1b2538; --radius:14px; --overlay:rgba(0,0,0,.45);
  }}
  :root[data-theme="light"] {{
    --bg:#f6f7fb; --panel:#ffffff; --panel-2:#f2f4f9; --text:#10182a; --muted:#4b5563;
    --ok:#158f60; --warn:#9b750f; --down:#b1353c; --unknown:#6b7280;
    --outline:#d5d8e1; --chip:#eef2f7; --overlay:rgba(0,0,0,.20);
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
  .header .right{{display:flex;align-items:center;gap:8px}}
  .btn, .theme-toggle{{border:1px solid var(--outline);background:var(--panel-2);padding:6px 10px;border-radius:10px;cursor:pointer}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px}}
  .card{{border:1px solid var(--outline);border-radius:var(--radius);background:var(--panel);padding:14px}}
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
  .actions .btn:hover{{filter:brightness(1.05)}}
  .spark{{height:34px;border-radius:8px;background:var(--panel-2);border:1px dashed var(--outline);
    display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:12px;margin-bottom:10px}}
  .footer{{color:var(--muted);font-size:12px;margin-top:12px}}

  /* Settings Drawer */
  .drawer-overlay{{position:fixed;inset:0;background:var(--overlay);opacity:0;pointer-events:none;transition:.2s}}
  .drawer-overlay.active{{opacity:1;pointer-events:auto}}
  .drawer{{position:fixed;top:0;right:-720px;width:700px;max-width:95vw;height:100vh;background:var(--panel);
    border-left:1px solid var(--outline);box-shadow:0 0 30px rgba(0,0,0,.3);transition:right .25s}}
  .drawer.active{{right:0}}
  .drawer header{{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;border-bottom:1px solid var(--outline)}}
  .drawer .body{{padding:14px;height:calc(100vh - 56px);overflow:auto}}
  .form-group{{margin-bottom:12px}}
  textarea{{width:100%;min-height:320px;background:var(--panel-2);color:var(--text);border:1px solid var(--outline);
    border-radius:10px;padding:10px;font-family:ui-monospace,Consolas,Menlo,monospace}}
  .row{{display:grid;grid-template-columns:1fr;gap:14px}}
  @media (min-width: 840px){{ .row{{grid-template-columns:1fr 1fr}} }}
  .help{{color:var(--muted);font-size:12px;margin-top:6px}}
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
        {chips_html}
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
      <a id="openSettings" href="#">Settings</a>
      <a href="logs/">Logs folder</a>
    </div>
  </aside>

  <main class="content">
    <div class="header">
      <h1>Targets Overview</h1>
      <div class="right">
        <div class="subtitle">Showing: <strong id="count">0</strong> • Range: <strong id="rangeLabel">{default_range}</strong></div>
        <button id="themeBtn" class="theme-toggle" title="Toggle Light/Dark">🌓 Theme</button>
        <button id="openSettings2" class="btn" title="Open Settings drawer">⚙️ Settings</button>
      </div>
    </div>

    <div class="grid" id="cards">
{cards_html}
    </div>

    <div class="footer">
      Generated: {generated_ts} — Auto-refresh: {refresh_state}
    </div>
  </main>
</div>

<!-- Settings Drawer -->
<div id="drawerOverlay" class="drawer-overlay"></div>
<div id="drawer" class="drawer">
  <header>
    <strong>Settings</strong>
    <button id="closeDrawer" class="btn">✖ Close</button>
  </header>
  <div class="body">
    <div class="help">
      Edit the YAML files below, then <strong>Download</strong> them and replace on the server:
      <ul>
        <li><code>{settings_path}</code> — script settings</li>
        <li><code>{targets_path}</code> — targets</li>
      </ul>
      This dashboard is static and cannot write files to disk.
    </div>

    <div class="row">
      <div class="form-group">
        <h3>mtr_script_settings.yaml</h3>
        <textarea id="settingsTa">{settings_text}</textarea>
        <div class="help">Ranges shown on the sidebar come from <code>html.ranges</code> (via <code>get_html_ranges()</code>).</div>
        <div style="margin-top:8px">
          <button class="btn" onclick="downloadYaml('mtr_script_settings.yaml', document.getElementById('settingsTa').value)">⬇ Download settings.yaml</button>
        </div>
      </div>

      <div class="form-group">
        <h3>mtr_targets.yaml</h3>
        <textarea id="targetsTa">{targets_text}</textarea>
        <div class="help">Each target supports <code>ip</code> and optional <code>description</code>.</div>
        <div style="margin-top:8px">
          <button class="btn" onclick="downloadYaml('mtr_targets.yaml', document.getElementById('targetsTa').value)">⬇ Download targets.yaml</button>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
  // --- Theme toggle with localStorage ---
  (function initTheme(){{
    const saved = localStorage.getItem('mtr_theme') || 'dark';
    if (saved === 'light') document.documentElement.setAttribute('data-theme','light');
    document.getElementById('themeBtn').addEventListener('click', () => {{
      const cur = document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
      const next = (cur === 'light') ? 'dark' : 'light';
      if (next === 'light') document.documentElement.setAttribute('data-theme','light');
      else document.documentElement.removeAttribute('data-theme');
      localStorage.setItem('mtr_theme', next);
    }});
  }})();

  // --- Search + Status filters + Range label (cosmetic) ---
  const q = document.getElementById('q');
  const cards = document.getElementById('cards');
  const rangeLabel = document.getElementById('rangeLabel');
  const countEl = document.getElementById('count');

  function updateVisibleCount(){{
    const visible = Array.from(cards.children).filter(el => el.style.display !== 'none').length;
    countEl.textContent = String(visible);
  }}

  q.addEventListener('input', () => {{
    const term = q.value.toLowerCase();
    Array.from(cards.children).forEach(c => {{
      const ip = (c.dataset.ip || '').toLowerCase();
      const desc = (c.querySelector('.desc')?.textContent || '').toLowerCase();
      c.style.display = (ip.includes(term) || desc.includes(term)) ? '' : 'none';
    }});
    updateVisibleCount();
  }});

  document.querySelectorAll('.chip[data-status]').forEach(chip => {{
    chip.addEventListener('click', () => {{
      const s = chip.dataset.status;
      const active = chip.classList.toggle('active');
      document.querySelectorAll('.chip[data-status]').forEach(c => {{ if (c!==chip) c.classList.remove('active'); }});
      Array.from(cards.children).forEach(c => {{
        c.style.display = (!active || c.dataset.status === s) ? '' : 'none';
      }});
      updateVisibleCount();
    }});
  }});

  document.querySelectorAll('.chip[data-range]').forEach(chip => {{
    chip.addEventListener('click', () => {{
      rangeLabel.textContent = chip.dataset.range;
      // Future: re-render in-card sparklines for the chosen range.
    }});
  }});

  // Initial count
  updateVisibleCount();

  // --- Settings Drawer logic ---
  const drawer = document.getElementById('drawer');
  const overlay = document.getElementById('drawerOverlay');
  function openDrawer(){{ drawer.classList.add('active'); overlay.classList.add('active'); }}
  function closeDrawer(){{ drawer.classList.remove('active'); overlay.classList.remove('active'); }}
  document.getElementById('openSettings').addEventListener('click', (e)=>{{ e.preventDefault(); openDrawer(); }});
  document.getElementById('openSettings2').addEventListener('click', (e)=>{{ e.preventDefault(); openDrawer(); }});
  document.getElementById('closeDrawer').addEventListener('click', closeDrawer);
  overlay.addEventListener('click', closeDrawer);

  // --- Static "download file" helpers ---
  function downloadYaml(filename, text) {{
    const blob = new Blob([text], {{ type: 'text/yaml' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click();
    setTimeout(() => {{ URL.revokeObjectURL(url); a.remove(); }}, 0);
  }}
  window.downloadYaml = downloadYaml;
</script>

</body>
</html>
""".format(
        meta_refresh=meta_refresh,
        chips_html=chips_html,
        default_range=html_escape(default_range_label),
        cards_html=cards_html,
        generated_ts=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        refresh_state=("enabled" if auto_refresh_seconds > 0 else "disabled"),
        settings_path=html_escape(settings_path),
        targets_path=html_escape(targets_path),
        settings_text=settings_text,
        targets_text=targets_text,
    )

    # Atomic write (prevents partial/blank pages if errors occur mid-write)
    try:
        _atomic_write(index_path, page)
        logger.info(f"[index] Wrote {index_path} with {len(cards)} targets and embedded Settings drawer.")
    except Exception as e:
        logger.error(f"[index] Failed to write {index_path}: {e}")
