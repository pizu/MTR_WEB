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


class PipelineRunner:
    def __init__(self, repo_root: str, scripts: List[str], settings_file: str, logger):
        self.repo_root     = repo_root
        self.scripts       = scripts
        self.settings_file = settings_file
        self.logger        = logger
        self.python        = sys.executable or "/usr/bin/python3"

    def _run_one(self, script_path: str) -> bool:
        """Run a single step (returns True on success, False on failure)."""
        name = os.path.basename(script_path)
        args = [self.python, script_path, "--settings", self.settings_file]
        try:
            self.logger.info(f"[pipeline] Running {name} …")
            completed = subprocess.run(
                args,
                cwd=self.repo_root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False
            )
            if completed.returncode != 0:
                self.logger.error(f"[pipeline] {name} failed with rc={completed.returncode}")
                return False
            self.logger.info(f"[pipeline] {name} OK")
            return True
        except Exception as e:
            self.logger.error(f"[pipeline] {name} crashed: {e}")
            return False

    def run_all(self) -> bool:
        """Run all steps in order; stop at first failure."""
        for script in self.scripts:
            if not self._run_one(script):
                return False
        return True
