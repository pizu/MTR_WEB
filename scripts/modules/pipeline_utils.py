"""
modules/pipeline_utils.py
-------------------------
Contains PipelineRunner, which executes the "reporting pipeline" in order:

    graph_generator.py  →  timeseries_exporter.py  →  html_generator.py  →  index_generator.py

Each step is executed as a separate Python process with the shared --settings <yaml>.
If any step fails (non‑zero return code or exception), the pipeline stops immediately
and returns False to the caller. Success returns True.

Notes:
- stdout/stderr are suppressed here because each child script should log to its own file
  via modules.utils.setup_logger. If you need live console output, change DEVNULL to None.
"""

import os
import sys
import subprocess
from typing import List
from datetime import datetime
from modules.utils import load_settings

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


class PipelineRunner:
    def __init__(self, repo_root: str, scripts: List[str], settings_file: str, logger):
        self.repo_root     = repo_root
        self.scripts       = scripts
        self.settings_file = settings_file
        self.logger        = logger
        self.python        = sys.executable or "/usr/bin/python3"
        # Put pipeline logs under the main logs dir if configured; else ./logs
        # We don’t import utils here to keep this module minimal.
        try:
            cfg = load_settings(self.settings_file)
            default_logs = cfg.get("log_directory", os.path.join(self.repo_root, "logs"))
        except Exception:
            default_logs = os.path.join(self.repo_root, "logs")
            _ensure_dir(default_logs)
            self.pipeline_log_dir = os.environ.get("PIPELINE_LOG_DIR", default_logs)

    def _step_log_path(self, script_basename: str) -> str:
        return os.path.join(self.pipeline_log_dir, f"pipeline_{script_basename}.log")

    def _run_one(self, script_path: str) -> bool:
        """Run a single step with stdout/stderr to a dedicated log file."""
        name = os.path.basename(script_path)
        args = [self.python, script_path, "--settings", self.settings_file]
        step_log = self._step_log_path(name)

        self.logger.info(f"[pipeline] Running {name} …  (log: {step_log})")
        # Open the logfile in append, include a start banner with time
        with open(step_log, "a", encoding="utf-8", errors="replace") as lf:
            lf.write(f"\n=== {datetime.now().isoformat(timespec='seconds')} | START {name} ===\n")
            lf.write(f"$ {' '.join(args)}\n")
            lf.flush()
            try:
                completed = subprocess.run(
                    args,
                    cwd=self.repo_root,
                    stdout=lf,
                    stderr=lf,
                    check=False
                )
                rc = completed.returncode
            except Exception as e:
                lf.write(f"[EXCEPTION] {e}\n")
                rc = 255

            if rc != 0:
                self.logger.error(f"[pipeline] {name} failed with rc={rc}")
                # Show the last ~20 lines to the controller log for quick context
                try:
                    with open(step_log, "r", encoding="utf-8", errors="replace") as rf:
                        tail = rf.read().splitlines()[-20:]
                    if tail:
                        self.logger.error("[pipeline] --- tail of {} ---".format(step_log))
                        for line in tail:
                            self.logger.error(line)
                        self.logger.error("[pipeline] --- end tail ---")
                except Exception as te:
                    self.logger.error(f"[pipeline] Unable to read tail of {step_log}: {te}")
                return False

            self.logger.info(f"[pipeline] {name} OK")
            return True

    def run_all(self) -> bool:
        """Run all steps in order; stop at first failure."""
        for script in self.scripts:
            if not self._run_one(script):
                return False
        return True
