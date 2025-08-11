#!/usr/bin/env python3
\"\"\"
cleanup.py
Deletes old RRD, log, traceroute, graph, and HTML files based on per-type retention
defined in mtr_script_settings.yaml. Uses shared logger from utils.py.

Enhancement: adds a safety buffer so we never delete very recent files that may still
be in use - specifically, PNG graphs newer than 2x RRD step (configurable).
\"\"\"

import os
import sys
import time
from datetime import datetime

# Ensure local imports work no matter the CWD
SCRIPT_DIR = os.path.dirname(__file__)
MODULES_DIR = os.path.join(SCRIPT_DIR, "modules")
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, MODULES_DIR)

from modules.utils import load_settings, setup_logger

# Load settings and logger
settings = load_settings("mtr_script_settings.yaml")
logger = setup_logger("cleanup", settings.get("log_directory", "/tmp"), "cleanup.log", settings=settings)

# Directories from settings
RRD_DIR         = settings.get("rrd_directory", "data")
TRACEROUTE_DIR  = settings.get("traceroute_directory", "traceroute")
GRAPH_DIR       = settings.get("graph_output_directory", "html/graphs")
HTML_DIR        = "html"
LOG_DIR         = settings.get("log_directory", "logs")

# Retention days per type
retention = settings.get("retention", {})

# Safety buffer for very recent files, focused on graphs.
# Default: 2 x RRD step (in seconds); override-able via `graph_cleanup_safety_seconds` in YAML.
rrd_step = 60
try:
    rrd_cfg = settings.get("rrd", {})
    rrd_step = int(rrd_cfg.get("step", 60))
except Exception:
    pass

GRAPH_SAFETY_SECONDS = int(settings.get("graph_cleanup_safety_seconds", rrd_step * 2))

def is_older_than(path, cutoff_ts):
    try:
        return os.path.getmtime(path) < cutoff_ts
    except FileNotFoundError:
        return False
    except Exception as e:
        logger.warning(f\"mtime check failed for {path}: {e}\")
        return False

def matches_extension(filename, extensions):
    if not extensions:
        return True
        lower = filename.lower()
        return any(lower.endswith(ext) for ext in extensions)
        
def cleanup_dir(path, days, extensions=None, label=None, min_age_seconds=0):
    \"\"\"
    Deletes files under `path` that match `extensions` and are older than `days`.
    Won't delete files newer than `min_age_seconds` (relative to now).
    
    - days: integer days; if None or <= 0, cleanup for that type is skipped.
    - extensions: list like [".png"] or [".rrd"]
    - label: friendly label used in logs
    - min_age_seconds: safety buffer; files newer than now - min_age_seconds are kept.
    \"\"\"
    if not days or days <= 0:
        logger.info(f\"[{label}] Skipped — retention not set.\")
            return

        if not os.path.isdir(path):
            logger.info(f\"[{label}] Skipped — directory not found: {path}\")
            return

        now = time.time()
        cutoff = now - (days * 86400)
        safety_cutoff = now - min_age_seconds if min_age_seconds > 0 else None

        deleted = 0
        scanned = 0
        for root, dirs, files in os.walk(path):
            for f in files:
                if not matches_extension(f, extensions):
                    continue
                    
                    full = os.path.join(root, f)
                    scanned += 1
                    
                    # Apply safety buffer for "too new" files
                    if safety_cutoff is not None:
                        try:
                            mtime = os.path.getmtime(full)
                        except FileNotFoundError:
                            continue
                        except Exception as e:
                            logger.warning(f\"[{label}] Failed mtime for {full}: {e}\")
                                           continue

                    if mtime >= safety_cutoff:
                        # Too recent - keep it
                        continue

                # Check retention cutoff
                try:
                    mtime = os.path.getmtime(full)
                except FileNotFoundError:
                    continue
                except Exception as e:
                    logger.warning(f\"[{label}] Failed mtime for {full}: {e}\")
                    continue

                if mtime < cutoff:
                    try:
                        os.remove(full)
                        deleted += 1
                    except Exception as e:
                        logger.warning(f\"[{label}] Failed to delete {full}: {e}\")
        logger.info(f\"[{label}] Scanned {scanned} file(s). Deleted {deleted} older than {days} day(s).\" + (f\" Safety buffer: {min_age_seconds}s.\" if min_age_seconds else \"\"))

    def main():
        logger.info(\"===== Cleanup started =====\")
        cleanup_dir(RRD_DIR,        retention.get(\"rrd_days\"),        [\".rrd\"],  \"RRD files\")
        cleanup_dir(LOG_DIR,        retention.get(\"logs_days\"),       [\".log\"],  \"Logs\")
        cleanup_dir(TRACEROUTE_DIR, retention.get(\"traceroute_days\"), [\".json\"], \"Traceroutes\")
        # Apply safety buffer to PNG graphs to avoid trimming files still being updated
        cleanup_dir(GRAPH_DIR,      retention.get(\"graphs_days\"),     [\".png\"],  \"Graphs\", min_age_seconds=GRAPH_SAFETY_SECONDS)
        cleanup_dir(HTML_DIR,       retention.get(\"html_days\"),       [\".html\"], \"HTML pages\")
        logger.info(\"===== Cleanup finished =====\")

    if __name__ == \"__main__\":
        main()
    """)

cleanup_file.write_text(new_code_ascii, encoding="utf-8")

print("Updated cleanup.py with a safety buffer for recent graph PNGs.")
print("File:", str(cleanup_file))
