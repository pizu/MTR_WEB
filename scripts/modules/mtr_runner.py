#!/usr/bin/env python3
"""
modules/mtr_runner.py

What this module does:
  - Provides a single function `run_mtr(...)` that executes one MTR snapshot
    and returns a normalized list of hop dicts (ready for RRD updates, logging, etc.)
  - Spawns the external 'mtr' command in its OWN process-group so we can stop it
    immediately (TERM → KILL) during shutdown.
  - Applies timeouts and safe cleanup so no zombie 'mtr' processes linger.

Why we care about process-groups:
  - Our controller and watchdog use process-group signaling to kill entire trees
    (watchdog → mtr → rrdtool, etc.). To be killable as a group, each subprocess
    should start with start_new_session=True so it becomes the leader of a new
    session/process-group. Then os.killpg() can stop everything in one go.

Compatible with monitor.py (you shared monitor (4).py):
  - Signature: run_mtr(ip, source_ip, logger, settings) -> list[dict]
  - Returns [] on failure so the monitor can just sleep and retry.
"""

import os
import json
import shlex
import signal
import subprocess
from typing import Any, Dict, List, Optional


def _compute_timeout_seconds(settings: dict) -> int:
    """
    Compute a reasonable timeout for one mtr '--report' run, based on YAML.
    Settings used (with defaults):
      mtr.report_cycles       -> default 1
      mtr.packets_per_cycle   -> default 20 (maps to -c)
      mtr.per_packet_interval -> default 1.0 seconds (maps to -i)
      mtr.timeout_seconds     -> default 0 (auto)

    When timeout_seconds == 0 (auto), we derive:
        cycles * packets_per_cycle * per_packet_interval + 5s guard
    """
    mtr_cfg = settings.get("mtr", {}) or {}
    cycles = int(mtr_cfg.get("report_cycles", 1))
    packets = int(mtr_cfg.get("packets_per_cycle", 20))
    per_packet_interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    timeout_seconds = int(mtr_cfg.get("timeout_seconds", 0))
    if timeout_seconds > 0:
        return timeout_seconds
    est = int(cycles * packets * per_packet_interval + 5)
    return max(est, 10)  # safety floor


def _normalize_hub(h: dict) -> dict:
    """
    Normalize one 'hub' entry from mtr --json output into our standard shape.
    We preserve the original keys where possible but make sure types are correct.

    Example input from mtr:
      {
        "count": 2,
        "host": "1.2.3.4",
        "Loss%": 0.0, "Snt": 10,
        "Last": 12.5, "Avg": 13.2, "Best": 10.1, "Wrst": 15.8, "StDev": 1.1
      }
    """
    return {
        "count": int(h.get("count", 0)),
        "host": h.get("host", "???"),
        "Loss%": float(h.get("Loss%", 0.0) or 0.0),
        "Snt": int(h.get("Snt", 0) or 0),
        "Last": float(h.get("Last", 0.0) or 0.0),
        "Avg": float(h.get("Avg", 0.0) or 0.0),
        "Best": float(h.get("Best", 0.0) or 0.0),
        "Wrst": float(h.get("Wrst", 0.0) or 0.0),
        "StDev": float(h.get("StDev", 0.0) or 0.0),
    }


def run_mtr(ip: str,
            source_ip: Optional[str],
            logger,
            settings: dict) -> List[Dict[str, Any]]:
    """
    Run a single MTR report against `ip` and return a list of normalized hop dicts.

    Args:
      ip:        Destination IP/hostname to probe.
      source_ip: Optional source IP to bind (passed as --address).
      logger:    Project logger (we log warnings/errors here).
      settings:  Global YAML settings dict (mtr behavior/timeout/dns, etc.)

    Returns:
      List of hop dicts (possibly empty [] on failure). Each hop includes:
        count, host, Loss%, Snt, Last, Avg, Best, Wrst, StDev
    """
    # ---- Read MTR-related tunables from YAML (with safe defaults) ----
    mtr_cfg = settings.get("mtr", {}) or {}
    cycles = int(mtr_cfg.get("report_cycles", 1))              # how many snapshots
    packets_per_cycle = int(mtr_cfg.get("packets_per_cycle", 20))  # -c
    per_packet_interval = float(mtr_cfg.get("per_packet_interval", 1.0))  # -i
    resolve_dns = bool(mtr_cfg.get("resolve_dns", False))      # if false, use -n (numeric only)

    # ---- Build the mtr command line ----
    # We use --json and the "report" mode so we get a stable summary quickly.
    cmd: list[str] = ["mtr", "--json"]

    # -n disables DNS resolution for consistency unless the user enabled it
    if not resolve_dns:
        cmd.append("-n")

    # Number of packets per cycle and per-packet interval
    # (-c controls packets per cycle; -i controls spacing between packets)
    if packets_per_cycle > 0:
        cmd += ["-c", str(packets_per_cycle)]
    if per_packet_interval > 0:
        cmd += ["-i", str(per_packet_interval)]

    # --report-cycles controls how many *reports* mtr prints; we collect the last one
    if cycles > 1:
        cmd += ["--report-cycles", str(cycles)]

    # Bind to a specific source, if requested
    if source_ip:
        cmd += ["--address", str(source_ip)]

    # Finally the destination target
    cmd.append(str(ip))

    # For debugging, having the string is handy in logs
    cmd_str = " ".join(shlex.quote(x) for x in cmd)

    # ---- Execute the command in its own process-group (critical for clean stop) ----
    timeout = _compute_timeout_seconds(settings)
    try:
        # start_new_session=True => child becomes leader of a new session/process-group
        # text=True => stdout/stderr are decoded strings (UTF-8)
        proc = subprocess.Popen(
            cmd,
            start_new_session=True,      # <<< make it killable as a group
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Timed out: TERM the process-group; if it resists, KILL
            try:
                pgid = os.getpgid(proc.pid)
            except Exception:
                pgid = None
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                if pgid is not None:
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    proc.kill()
            logger.warning(f"[{ip}] MTR timed out after {timeout}s; cmd={cmd_str}")
            return []

        # Non-zero exit? Log and bail gracefully
        if proc.returncode != 0:
            logger.warning(f"[{ip}] mtr exited {proc.returncode}; stderr: {stderr.strip() or '(none)'}; cmd={cmd_str}")
            return []

        if not stdout.strip():
            logger.warning(f"[{ip}] mtr produced no output; cmd={cmd_str}")
            return []

        # ---- Parse JSON and normalize hubs ----
        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as je:
            logger.warning(f"[{ip}] failed to parse mtr JSON: {je}; first 200 chars: {stdout[:200]!r}")
            return []

        # Expecting {"report": {"hubs": [ ... ]}}
        report = (data or {}).get("report") or {}
        hubs = report.get("hubs") or []
        if not isinstance(hubs, list) or not hubs:
            # Some failures print an empty hubs list even on success
            logger.warning(f"[{ip}] mtr JSON has no hubs; cmd={cmd_str}")
            return []

        normalized = [_normalize_hub(h) for h in hubs]
        return normalized

    except FileNotFoundError:
        # mtr binary missing
        logger.error("mtr command not found. Please install 'mtr' (e.g., 'dnf install mtr' or 'apt install mtr').")
        return []
    except Exception as e:
        # Any other unexpected failure
        logger.exception(f"[{ip}] Unexpected error running mtr: {e}")
        return []
