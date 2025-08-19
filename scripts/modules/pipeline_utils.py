"""
modules/pipeline_utils.py
-------------------------
PipelineRunner: run the reporting pipeline in order:
  graph_generator.py → timeseries_exporter.py → html_generator.py → index_generator.py
"""

import os
import sys
import subprocess
from typing import List


class PipelineRunner:
    def __init__(self, repo_root: str, scripts: List[str], settings_file: str, logger):
        self.repo_root    = repo_root
        self.scripts      = scripts
        self.settings_file = settings_file
        self.logger       = logger
        self.python       = sys.executable or "/usr/bin/python3"

    def _run_one(self, script_path: str) -> bool:
        """
        Run a single reporting script using the shared settings file.
        Each child is responsible for its own logging to files.
        """
        name = os.path.basename(script_path)
        args = [self.python, script_path, "--settings", self.settings_file]
        try:
            self.logger.info(f"[pipeline] Running {name} …")
            # Use run() so we get returncode reliably; pipe stdout/stderr to keep service quiet.
            completed = subprocess.run(
                args, cwd=self.repo_root,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
            )
            if completed.returncode != 0:
                self.logger.error(f"[pipeline] {name} failed with rc={completed.returncode}")
                return False
            self.logger.info(f"[pipeline] {name} OK")
            return True
        except Exception as e:
            self.logger.error(f"[pipeline] {name} failed: {e}")
            return False

    def run_all(self) -> bool:
        """
        Run all reporting scripts in order. Fail-fast: if one fails, stop here.
        Returns True if the whole pipeline succeeded.
        """
        for script in self.scripts:
            ok = self._run_one(script)
            if not ok:
                return False
        return True
