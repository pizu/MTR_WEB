#!/usr/bin/env python3
"""
modules/index_writer.py
-----------------------

Purpose
=======
Generate a modern `index.html` that visually matches the per-target pages while
adding a fixed left sidebar for search/filters and a card grid for targets.

What this script DOES:
  - Reads paths + knobs from `mtr_script_settings.yaml` (via your utils).
  - Reads targets from `mtr_targets.yaml` (upstream in index_generator.py).
  - Produces a fully self-contained HTML file (no external CSS/JS frameworks).
  - Optionally runs a *light* reachability check (fping) per IP.
  - Extracts "Last Seen" from each target's log (or falls back to file mtime).
  - Optionally counts hops from `<traceroute>/<ip>_hops.json` if available.
  - Keeps the layout consistent with modules/html_builder/target_html.py colors.

What this script does NOT do:
  - It does NOT render PNG graphs or rely on any PNG artifacts.
  - It does NOT probe MTR; it merely summarizes what other scripts produce.
  - It does NOT alter your logging configuration or files.

Inputs (from settings)
----------------------
We use your centralized utils to fetch the following resolved paths:
  - paths.logs       : directory with per-IP logs (`<ip>.log`)
  - paths.traceroute : directory where `<ip>_hops.json` may exist
  - resolve_html_dir : base HTML output directory for `index.html`

Other knobs:
  - html.auto_refresh_seconds (int) → <meta http-equiv="refresh"> for index
  - index_page.enable_fping_check (bool) → whether to call fping for status
  - paths.fping (str) → path to fping binary when status is enabled
  - ui.index_default_timerange (str, optional) → label shown in the header
    (purely cosmetic here; can be reused when you add sparklines on index)

Targets
-------
A list of dicts (from mtr_targets.yaml), each with at least:
  - ip (str)            : destination IP
  - description (str)   : optional human-friendly label

Generated HTML
--------------
Layout:
  [ fixed sidebar (search, time range chips, status filters, links) ] |
  [ main area with a responsive card grid, one card per target ]

Each card shows:
  - IP (with status chip: UP/WARN/DOWN/UNKNOWN)
  - Description (from targets file)
  - Meta line: Last Seen • Hops (if known) • Loss (placeholder "—")
  - Placeholder box for a future mini trend (no Chart.js required yet)
  - Buttons to open the per-target page and logs page

Implementation Notes
--------------------
- We keep HTML/CSS inlined to make deployment trivial.
- Colors/spacing are aligned with `target_html.py` so it feels unified.
- The JavaScript is minimal: client-side search, status filtering, and
  time range label switching (cosmetic for now).

Usage
-----
This module is used by `index_generator.py`:

    from modules.index_writer import generate_index_page
    generate_index_page(targets, settings, logger)

where `targets` come from `mtr_targets.yaml` and `settings` is loaded from
`mtr_script_settings.yaml` via your shared `modules.utils`.

"""

import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from modules.fping_status import get_fping_status
from modules.utils import resolve_html_dir, resolve_all_paths


def _read_last_seen_from_log(log_path: str) -> str:
    """
    Try to extract a human-readable "Last Seen" timestamp from the log.
    Strategy:
      1) Iterate log lines, keep the last line containing "MTR RUN".
      2) If not found, fall back to file modification time.
      3) If the file doesn't exist, return "Never".

    Returns a string formatted as "YYYY-MM-DD HH:MM:SS".
    """
    if not os.path.exists(log_path):
        return "Never"

    # Attempt #1: find latest "MTR RUN" line.
    last_line = None
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "MTR RUN" in line:
                    last_line = line.strip()
    except Exception:
        last_line = None

    if last_line:
        # Logs usually start with a timestamp. Many of your logs look like:
        # "2025-08-24 11:00:24 [INFO] [8.8.8.8] 15m (900s) ..."
        # We'll attempt to extract the leading timestamp if present.
        # Otherwise return the entire line to preserve context.
        parts = last_line.split(" [", 1)
        try:
            ts = parts[0].strip()
            # Best-effort sanity check: "YYYY-MM-DD HH:MM:SS"
            if len(ts) >= 19 and ts[4] == "-" and ts[7] == "-" and ts[10] == " ":
                return ts
        except Exception:
            pass
        return last_line

    # Attempt #2: fallback to file modification time.
    try:
        return datetime.fromtimestamp(os.path.getmtime(log_path)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"


def _read_hop_count(traceroute_dir: str, ip: str) -> Optional[int]:
    """
    If `<traceroute>/<ip>_hops.json` exists, return the count of hop records.
    The file format is produced by your traceroute/graph pipeline, typically:
        [ { "count": 0, "host": "192.0.2.1" }, ... ]
    If missing or on error, return None (we'll display '—' in UI).
    """
    path = os.path.join(traceroute_dir, f"{ip}_hops.json")
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f) or []
            # Count entries that look like hop records
            return len([1 for _ in arr])
    except Exception:
        pass
    return None


def _classify_status(raw: str) -> str:
    """
    Normalize fping result (which may be "alive", "unreachable", etc.) into
    one of: "up", "warn", "down", "unknown".

    - "alive"           → up
    - "unreachable"     → down
    - "loss>0 but <100" → warn (Only if you later provide loss % here)
    - anything else     → unknown

    NOTE: For now we map only alive/unreachable → up/down.
          If you later wire in "warn" conditions, keep this centralized.
    """
    if not raw:
        return "unknown"
    r = raw.strip().lower()
    if r == "alive":
        return "up"
    if r == "unreachable":
        return "down"
    # Placeholder for future "warn" mapping
    return "unknown"


def generate_index_page(targets: List[Dict[str, Any]], settings: Dict[str, Any], logger) -> None:
    """
    Build the index.html with a left sidebar and a right card grid.

    Parameters
    ----------
    targets  : list of dicts
        Parsed from mtr_targets.yaml (index_generator handles file IO).
    settings : dict
        Loaded from mtr_script_settings.yaml (via modules.utils.load_settings).
    logger   : logging.Logger
        Project-wide logger obtained from modules.utils.setup_logger.

    Output
    ------
    Writes `<HTML_DIR>/index.html`
    """
    # Resolve directories from settings using your shared utils.
    HTML_DIR = resolve_html_dir(settings)
    paths    = resolve_all_paths(settings)

    LOG_DIR         = paths["logs"]
    TRACE_DIR       = paths["traceroute"]
    FPING_PATH      = paths.get("fping")

    # Whether to use fping for a live up/down check on the index.
    # Backward-compatible fallback to legacy setting if needed.
    ENABLE_FPING = settings.get("index_page", {}).get(
        "enable_fping_check",
        settings.get("enable_fping_check", True)
    )

    # Index auto-refresh (in seconds). 0 disables auto-refresh meta tag.
    REFRESH_SECONDS = settings.get("html", {}).get(
        "auto_refresh_seconds",
        settings.get("html_auto_refresh_seconds", 0)
    )

    # Cosmetic time-range label for the header (optional, used later for sparklines)
    INDEX_RANGE_LABEL = settings.get("ui", {}).get("index_default_timerange", "15m")

    os.makedirs(HTML_DIR, exist_ok=True)
    index_path = os.path.join(HTML_DIR, "index.html")

    # Precompute all card data before writing HTML (clear separation of concerns).
    cards: List[Dict[str, str]] = []
    for t in targets:
        ip = (t or {}).get("ip") or ""
        if not ip:
            continue

        description = (t or {}).get("description", "") or ""
        log_path = os.path.join(LOG_DIR, f"{ip}.log")
        last_seen = _read_last_seen_from_log(log_path)

        # Reachability / Status
        status_raw = "Unknown"
        if ENABLE_FPING:
            try:
                status_raw = get_fping_status(ip, FPING_PATH)
            except Exception as e:
                logger.warning(f"fping status failed for {ip}: {e}")
                status_raw = "Unknown"

        status_class = _classify_status(status_raw)  # "up" | "down" | "warn" | "unknown"

        # Hop count (optional, derived from _hops.json)
        hop_count = _read_hop_count(TRACE_DIR, ip)
        hop_text  = str(hop_count) if hop_count is not None else "—"

        cards.append({
            "ip": ip,
            "desc": description,
            "status": status_class,     # normalized chip styling
            "status_label": (status_raw or "Unknown").upper(),  # visible label for the chip
            "last_seen": last_seen,
            "hops": hop_text,
            # "loss": "—"  # Placeholder; wire real % later if you export it to a quick index JSON
        })

    # Start writing HTML
    try:
        with open(index_path, "w", encoding="utf-8") as f:
            # --- <head> with optional auto-refresh ---
            f.write("<!doctype html><html lang='en'><head><meta charset='utf-8'>")
            if REFRESH_SECONDS and int(REFRESH_SECONDS) > 0:
                f.write(f"<meta http-equiv='refresh' content='{int(REFRESH_SECONDS)}'>")
                logger.info(f"[index] Auto-refresh enabled: {REFRESH_SECONDS}s")
            else:
                logger.info("[index] Auto-refresh disabled")

            f.write("""
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MTR • Dashboard</title>
<style>
  :root{
    --bg:#0f1420; --panel:#131a28; --panel-2:#0d1320; --text:#e9eef7; --muted:#9fb0c6;
    --ok:#1faa70; --warn:#d7a021; --down:#cf3b43; --unknown:#6b7280;
    --outline:#26324a; --chip:#1b2538; --radius:14px;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,Segoe UI,Roboto,Arial,sans-serif}
  a{color:inherit;text-decoration:none}
  .layout{display:grid;grid-template-columns:280px 1fr;min-height:100vh}
  .sidebar{
    background:linear-gradient(180deg,var(--panel),var(--panel-2));
    border-right:1px solid var(--outline);padding:16px;position:sticky;top:0;height:100vh;overflow:auto;
  }
  .brand{font-weight:700;font-size:18px;margin-bottom:10px}
  .subtitle{color:var(--muted);font-size:12px;margin-bottom:16px}
  .section{margin:14px 0;padding:12px;border:1px solid var(--outline);border-radius:var(--radius);background:#101829}
  .section h4{margin:0 0 8px 0;font-size:12px;letter-spacing:.06em;color:var(--muted);text-transform:uppercase}
  .search{display:flex;gap:8px}
  .search input{
    width:100%;padding:10px;border-radius:10px;border:1px solid var(--outline);
    background:#0b1220;color:var(--text)
  }
  .chips{display:flex;flex-wrap:wrap;gap:8px}
  .chip{
    padding:6px 10px;border-radius:999px;background:var(--chip);border:1px solid var(--outline);
    font-size:12px;cursor:pointer;user-select:none
  }
  .chip.ok{border-color:var(--ok);color:var(--ok)}
  .chip.warn{border-color:var(--warn);color:var(--warn)}
  .chip.down{border-color:var(--down);color:var(--down)}
  .chip.unknown{border-color:var(--unknown);color:var(--unknown)}
  .nav a{display:block;padding:10px;border-radius:10px;color:var(--text);opacity:.9}
  .nav a:hover{background:#0b1220}
  .content{padding:20px}
  .header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px;flex-wrap:wrap}
  .header h1{font-size:18px;margin:0}
  .grid{
    display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
    gap:14px
  }
  .card{
    border:1px solid var(--outline);border-radius:var(--radius);background:var(--panel);padding:14px
  }
  .card-top{display:flex;justify-content:space-between;gap:10px;align-items:center}
  .ip{font-weight:700}
  .status{font-size:12px;padding:4px 8px;border-radius:999px;border:1px solid var(--outline)}
  .status.ok{border-color:var(--ok);color:var(--ok)}
  .status.warn{border-color:var(--warn);color:var(--warn)}
  .status.down{border-color:var(--down);color:var(--down)}
  .status.unknown{border-color:var(--unknown);color:var(--unknown)}
  .desc{color:var(--muted);margin:6px 0 10px 0}
  .meta{color:var(--muted);font-size:12px;margin-bottom:10px}
  .actions{display:flex;gap:8px;flex-wrap:wrap}
  .btn{
    padding:8px 10px;border-radius:10px;background:#0b1220;border:1px solid var(--outline);font-size:13px
  }
  .btn:hover{filter:brightness(1.1)}
  .spark{
    height:34px;border-radius:8px;background:#0b1220;border:1px dashed var(--outline);
    display:flex;align-items:center;justify-content:center;color:#567;font-size:12px;margin-bottom:10px
  }
  .footer{color:var(--muted);font-size:12px;margin-top:12px}
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
        <div class="chip" data-range="15m">15m</div>
        <div class="chip" data-range="1h">1h</div>
        <div class="chip" data-range="6h">6h</div>
        <div class="chip" data-range="24h">24h</div>
        <div class="chip" data-range="1w">1w</div>
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
      <a href="logs/">Logs folder</a>
    </div>
  </aside>

  <main class="content">
    <div class="header">
      <h1>Targets Overview</h1>
      <div class="subtitle">Showing: <strong id="count">0</strong> targets • Range: <strong id="rangeLabel">""" + _escape(INDEX_RANGE_LABEL) + """</strong></div>
    </div>

    <div class="grid" id="cards">
""")

            # --- Card markup for each target (data-* used by tiny JS filters) ---
            for c in cards:
                ip   = _escape(c["ip"])
                desc = _escape(c["desc"])
                status_class = c["status"] if c["status"] in ("up", "down", "warn", "unknown") else "unknown"
                status_label = _escape(c["status_label"])
                last_seen = _escape(c["last_seen"])
                hops = _escape(c["hops"])

                # You can wire a logs HTML later; for now, at least ip.log exists.
                logs_href = f"logs/{ip}.log"  # directory listing or direct file fetch depending on your web server
                target_href = f"{ip}.html"

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
                    "          <a class='btn' href='{href}'>View Details</a>\n"
                    "          <a class='btn' href='{logs}'>Logs</a>\n"
                    "        </div>\n"
                    "      </div>\n".format(
                        ip=ip, status=status_class, label=status_label,
                        desc=desc, last=last_seen, hops=hops,
                        href=target_href, logs=logs_href
                    )
                )

            # --- Footer + JS (search + status filter + range chips) ---
            f.write("""
    </div>

    <div class="footer">
      Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """ — Auto-refresh: """ + ("enabled" if (REFRESH_SECONDS and int(REFRESH_SECONDS) > 0) else "disabled") + """
    </div>
  </main>
</div>

<script>
  // Basic interactivity: client-side search, status toggle, range label.
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
      // Deactivate other status chips when one is active
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
      // In the future: re-render sparklines using that range (e.g., 15m/1h/24h).
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


# ------------------------
# Small HTML helpers below
# ------------------------
def _escape(s: Any) -> str:
    """
    Minimal HTML escaping for text injection in attributes/innerHTML.
    (We avoid importing html module to keep this file standalone.)
    """
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
