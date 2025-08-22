#!/usr/bin/env python3
"""
mtr_watchdog.py
---------------
Per-target launcher invoked by controller.py.

Responsibilities (for dummies):
  1) Parse CLI args (--settings, --target, --source).
  2) Load YAML settings from mtr_script_settings.yaml.
  3) Initialize logging with a logger named 'mtr_watchdog' (+ per-target extra file).
  4) Import and call the actual monitor loop function (monitor_target) from your codebase.

Compatibility:
- This script tries multiple module paths to find a function called `monitor_target`:
    modules.monitor.monitor_target
    modules.mtr_monitor.monitor_target
    modules.mtr_runner.monitor_target
    modules.mtr.monitor_target
    mtr_monitor.monitor_target   (legacy file directly under scripts/)
- It will call the function with only the parameters it accepts among:
    ip, source_ip, settings, logger
  using Python introspection (inspect.signature), so older/newer variants still work.

Exit codes:
  0 = clean stop (Ctrl+C or monitor exits normally)
  1 = bad arguments / failed settings or logging initialization / no monitor found
  2 = monitor crashed with an unexpected exception (controller may restart it)

YAML reminder:
Ensure a level exists for this logger, e.g.:
  logging:
    levels:
      mtr_watchdog: ERROR
"""

import os
import sys
import argparse
import logging
import importlib
import inspect
from typing import Optional, Callable, Tuple, Dict, Any, List

# --- Ensure 'modules' is importable even when launched from systemd ---
SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT   = os.path.abspath(os.path.join(SCRIPTS_DIR, os.pardir))
MODULES_DIR = os.path.join(SCRIPTS_DIR, "modules")

for p in (SCRIPTS_DIR, MODULES_DIR, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

# --- Shared helpers (settings + logging) ---
try:
    from modules.utils import load_settings, setup_logger  # refresh_logger_levels not needed here
except Exception as e:
    print(f"[FATAL] Cannot import modules.utils: {e}", file=sys.stderr)
    sys.exit(1)


def _candidates() -> List[Tuple[str, str]]:
    """Ordered list of (module_path, attr_name) to try for the monitor entrypoint."""
    return [
        ("modules.monitor", "monitor_target"),
        ("modules.mtr_monitor", "monitor_target"),
        ("modules.mtr_runner", "monitor_target"),
        ("modules.mtr", "monitor_target"),
        ("mtr_monitor", "monitor_target"),  # legacy scripts/mtr_monitor.py
    ]


def _import_monitor_target() -> Tuple[Callable[..., Any], str]:
    """
    Try importing monitor_target from various modules.
    Returns: (callable, "module_path.attr")
    Raises: ImportError with a combined message if none match.
    """
    errors = []
    for mod_path, attr in _candidates():
        try:
            mod = importlib.import_module(mod_path)
            func = getattr(mod, attr)
            if callable(func):
                return func, f"{mod_path}.{attr}"
            errors.append(f"{mod_path}.{attr} found but not callable")
        except Exception as e:
            errors.append(f"{mod_path}.{attr}: {e}")
    raise ImportError("None of the candidate monitor entrypoints could be imported:\n  - " + "\n  - ".join(errors))


def parse_args() -> argparse.Namespace:
    """
    CLI:
      --settings: path to mtr_script_settings.yaml
      --target  : required IP/host
      --source  : optional source IP to bind
    """
    default_settings = os.path.join(REPO_ROOT, "mtr_script_settings.yaml")
    parser = argparse.ArgumentParser(description="Launch per-target MTR monitor loop.")
    parser.add_argument("--settings", default=default_settings,
                        help=f"Path to YAML settings (default: {default_settings})")
    parser.add_argument("--target", required=True,
                        help="Destination host/IP to monitor (e.g., 8.8.8.8)")
    parser.add_argument("--source", default=None,
                        help="Optional source IP to bind (passed to mtr)")
    return parser.parse_args()


def _call_monitor_compat(func: Callable[..., Any],
                         ip: str,
                         source_ip: Optional[str],
                         settings: Dict[str, Any],
                         logger: logging.Logger) -> None:
    """
    Call the discovered monitor function with only the kwargs it accepts.
    Supported keywords: ip, target, source_ip, source, settings, logger
    """
    sig = inspect.signature(func)
    supported = set(sig.parameters.keys())
    kwargs = {}
    if "ip" in supported:         kwargs["ip"] = ip
    if "target" in supported:     kwargs["target"] = ip  # tolerate older name
    if "source_ip" in supported:  kwargs["source_ip"] = source_ip
    if "source" in supported:     kwargs["source"] = source_ip  # tolerate older name
    if "settings" in supported:   kwargs["settings"] = settings
    if "logger" in supported:     kwargs["logger"] = logger
    func(**kwargs)


def main() -> int:
    # 1) Parse args
    args = parse_args()

    # 2) Load settings
    try:
        settings = load_settings(args.settings)
    except Exception as e:
        print(f"[FATAL] Failed to load settings '{args.settings}': {e}", file=sys.stderr)
        return 1

    # 3) Initialize logging using new signature (dir & filename resolved from YAML)
    #    - Directory:   paths.logs (or legacy log_directory)
    #    - File name:   logging.files.mtr_watchdog (fallback "mtr_watchdog.log")
    #    - Extra file:  one per target (e.g., "8.8.8.8.log") alongside the main watchdog log
    try:
        logger = setup_logger(
            "mtr_watchdog",
            settings=settings,
            extra_file=f"{args.target}.log"
        )
    except Exception as e:
        print(f"[FATAL] Failed to initialize logging: {e}", file=sys.stderr)
        return 1

    logger.info(f"[{args.target}] Watchdog starting (settings='{args.settings}', source='{args.source}')")

    # 4) Import monitor entrypoint
    try:
        monitor_target, origin = _import_monitor_target()
        logger.debug(f"[{args.target}] Using monitor entrypoint: {origin}")
    except Exception as e:
        logger.error(f"[{args.target}] {e}")
        return 1

    # 5) Call the monitor with compatibility wrapper
    try:
        _call_monitor_compat(
            monitor_target,
            ip=args.target,
            source_ip=args.source,
            settings=settings,
            logger=logger,
        )
        logger.info(f"[{args.target}] Monitor exited normally.")
        return 0

    except KeyboardInterrupt:
        logger.info(f"[{args.target}] Stopped by user (KeyboardInterrupt).")
        return 0

    except SystemExit as e:
        code = int(getattr(e, "code", 0) or 0)
        if code == 0:
            logger.info(f"[{args.target}] Monitor requested clean exit.")
        else:
            logger.warning(f"[{args.target}] Monitor requested exit with code {code}.")
        return code

    except Exception as e:
        logger.exception(f"[{args.target}] Monitor crashed: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
