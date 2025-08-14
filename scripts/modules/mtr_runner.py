#!/usr/bin/env python3
"""
modules/mtr_runner.py

Purpose
-------
Run ONE MTR snapshot and return a normalized list of hop dicts.

Key design choices
------------------
1) We use `--json` + `--report-cycles` (NO `--report` flag), which is friendly
   across builds and gives a predictable runtime:
      total runtime ≈ cycles * packets_per_cycle * per_packet_interval

2) We start the subprocess with `start_new_session=True`, so the controller
   (or watchdog) can terminate the entire *process group* instantly on stop.

3) Timeout:
   - If mtr.timeout_seconds > 0, we use it as-is.
   - Else we compute: cycles * packets * interval * multiplier + margin,
     then apply a safety floor. This keeps the controller loop snappy
     without spurious timeouts on longer paths.

Inputs
------
run_mtr(ip, source_ip, logger, settings)
- ip:         target host/IP
- source_ip:  optional source address to bind (passed via --address)
- logger:     project logger
- settings:   dict loaded from mtr_script_settings.yaml

Returns
-------
[] on failure; otherwise a list of hops with consistent types:
  {
    "count": int,  # hop index from MTR output (usually 1-based)
    "host":  str,  # hostname or IP or "???"
    "Loss%": float,
    "Snt":   int,
    "Last":  float,
    "Avg":   float,
    "Best":  float,
    "Wrst":  float,
    "StDev": float
  }
"""

import os
import json
import shlex
import signal
import subprocess
import ipaddress
from typing import Any, Dict, List, Optional


# ----------------------------
# Helpers: timeout calculation
# ----------------------------
def _compute_timeout_report_mode(settings: dict) -> tuple[int, str]:
    """
    Compute a timeout suitable for --json with --report-cycles.

    YAML knobs (all under mtr.*):
      report_cycles (int)          default 1
      packets_per_cycle (int)      default 10
      per_packet_interval (float)  default 1.0
      timeout_seconds (int)        default 0 (auto)
      timeout_multiplier (float)   default 1.0
      timeout_margin_seconds (int) default 5
      timeout_floor_seconds (int)  default 10

    Returns (timeout_seconds, rationale_string)
    """
    mtr_cfg = settings.get("mtr", {}) or {}

    cycles   = int(mtr_cfg.get("report_cycles", 1))
    packets  = max(1, int(mtr_cfg.get("packets_per_cycle", 10)))
    interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    if interval <= 0:
        interval = 0.1

    # Fixed timeout wins
    fixed = int(mtr_cfg.get("timeout_seconds", 0))
    if fixed > 0:
        return fixed, f"fixed={fixed}s"

    multiplier = float(mtr_cfg.get("timeout_multiplier", 1.0))
    margin     = int(mtr_cfg.get("timeout_margin_seconds", 5))
    floor_s    = int(mtr_cfg.get("timeout_floor_seconds", 10))

    base = cycles * packets * interval
    est  = int(base * max(multiplier, 0.1)) + max(margin, 0)
    timeout = max(est, floor_s)

    why = (f"auto: cycles={cycles} * packets={packets} * interval={interval}s"
           f" * mult={multiplier} + margin={margin}s -> {est}s ; floor={floor_s}s"
           f" => timeout={timeout}s")
    return timeout, why


# ----------------------------
# Helpers: output normalization
# ----------------------------
def _normalize_hub(h: dict) -> dict:
    """Coerce MTR hub fields into consistent types."""
    return {
        "count": int(h.get("count", 0)),
        "host": h.get("host", "???"),
        "Loss%": float(h.get("Loss%", 0.0) or 0.0),
        "Snt":   int(h.get("Snt", 0) or 0),
        "Last":  float(h.get("Last", 0.0) or 0.0),
        "Avg":   float(h.get("Avg", 0.0) or 0.0),
        "Best":  float(h.get("Best", 0.0) or 0.0),
        "Wrst":  float(h.get("Wrst", 0.0) or 0.0),
        "StDev": float(h.get("StDev", 0.0) or 0.0),
    }


# ----------------------------
# Main entrypoint
# ----------------------------
def run_mtr(ip: str,
            source_ip: Optional[str],
            logger,
            settings: dict) -> List[Dict[str, Any]]:
    """
    Execute one MTR snapshot with predictable timing and clean cancellation.
    """

    # 1) Read knobs from YAML
    mtr_cfg = settings.get("mtr", {}) or {}
    cycles   = int(mtr_cfg.get("report_cycles", 1))
    packets  = max(1, int(mtr_cfg.get("packets_per_cycle", 10)))
    interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    if interval <= 0:
        interval = 0.1
    resolve_dns = bool(mtr_cfg.get("resolve_dns", False))

    # 2) Build command (NO --report; YES --report-cycles)
    #    This form is portable, keeps JSON clean, and gives constant-time runs.
    cmd: list[str] = ["mtr", "--json"]
    if not resolve_dns:
        cmd.append("-n")
    cmd += ["-c", str(packets)]
    cmd += ["-i", str(interval)]
    cmd += ["--report-cycles", str(cycles)]    # always include for predictability

    # Optional: when a source is provided, hint family (-4/-6) to avoid mtr guessing
    if source_ip:
        try:
            fam = ipaddress.ip_address(source_ip).version
            cmd.insert(1, "-6" if fam == 6 else "-4")  # insert right after "mtr"
        except Exception:
            pass
        cmd += ["--address", str(source_ip)]

    cmd.append(str(ip))
    cmd_str = " ".join(shlex.quote(x) for x in cmd)

    # 3) Compute timeout and log the rationale (great for troubleshooting)
    timeout, why = _compute_timeout_report_mode(settings)
    if logger:
        logger.debug(f"[{ip}] MTR cmd: {cmd_str}")
        logger.debug(f"[{ip}] timeout calc → {why}")

    # 4) Run MTR in its own process group; handle timeouts cleanly
    try:
        proc = subprocess.Popen(
            cmd,
            start_new_session=True,         # critical: kill the whole group on stop
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            # TERM the group; escalate to KILL if it ignores us
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
            if logger:
                logger.warning(f"[{ip}] MTR timed out after {timeout}s; cmd={cmd_str}")
            return []

        # 5) Non-zero exit → warn and return []
        if proc.returncode != 0:
            if logger:
                logger.warning(
                    f"[{ip}] mtr exited {proc.returncode}; "
                    f"stderr: {(stderr or '').strip() or '(none)'}; cmd={cmd_str}"
                )
            return []

        # 6) Basic JSON sanity
        if not stdout or not stdout.lstrip().startswith("{"):
            if logger:
                snip = (stdout or "").strip()[:400].replace("\n", "\\n")
                logger.warning(f"[{ip}] mtr produced non-JSON/empty output; stdout_snip={snip} ; cmd={cmd_str}")
            return []

        # 7) Parse and normalize hubs
        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as je:
            if logger:
                logger.warning(f"[{ip}] failed to parse JSON: {je}; first 200 chars: {stdout[:200]!r}")
            return []

        report = (data or {}).get("report") or {}
        hubs = report.get("hubs") or []
        if not isinstance(hubs, list) or not hubs:
            if logger:
                logger.warning(f"[{ip}] mtr JSON has no hubs; cmd={cmd_str}")
            return []

        return [_normalize_hub(h) for h in hubs]

    except FileNotFoundError:
        if logger:
            logger.error("mtr command not found. Install it (e.g., 'dnf install mtr' or 'apt install mtr').")
        return []
    except Exception as e:
        if logger:
            logger.exception(f"[{ip}] Unexpected error running mtr: {e}")
        return []
