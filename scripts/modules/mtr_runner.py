#!/usr/bin/env python3
"""
modules/mtr_runner.py

Runs ONE MTR snapshot and returns normalized hops.
- Prefers --json + --report-cycles (predictable runtime).
- Falls back to plain --json (non-report), with hop-aware timeout.
- Launches mtr in its own process group for clean cancellation.
"""

import os
import json
import shlex
import signal
import subprocess
import ipaddress
from typing import Any, Dict, List, Optional, Tuple


# ---------- Timeout calculators ----------
def _timeout_report_mode(settings: dict) -> Tuple[int, str]:
    """Timeout for --json with --report-cycles."""
    mtr_cfg = settings.get("mtr", {}) or {}
    cycles   = int(mtr_cfg.get("report_cycles", 1))
    packets  = max(1, int(mtr_cfg.get("packets_per_cycle", 10)))
    interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    if interval <= 0:
        interval = 0.1

    fixed = int(mtr_cfg.get("timeout_seconds", 0))
    if fixed > 0:
        return fixed, f"fixed={fixed}s"

    mult   = float(mtr_cfg.get("timeout_multiplier", 1.0))
    margin = int(mtr_cfg.get("timeout_margin_seconds", 5))
    floor_ = int(mtr_cfg.get("timeout_floor_seconds", 10))

    base = cycles * packets * interval
    est  = int(base * max(mult, 0.1)) + max(margin, 0)
    t    = max(est, floor_)
    why  = (f"report-mode: cycles={cycles}*packets={packets}*interval={interval}s"
            f"*mult={mult}+margin={margin}s -> {est}s; floor={floor_}s => {t}s")
    return t, why


def _timeout_nonreport_mode(settings: dict) -> Tuple[int, str]:
    """Timeout for plain --json (no --report-cycles): packets are per-hop."""
    mtr_cfg = settings.get("mtr", {}) or {}
    packets = max(1, int(mtr_cfg.get("packets_per_cycle", 10)))
    interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    if interval <= 0:
        interval = 0.1
    est_hops = max(1, int(settings.get("max_hops", mtr_cfg.get("max_hops", 30))))

    fixed = int(mtr_cfg.get("timeout_seconds", 0))
    if fixed > 0:
        return fixed, f"fixed={fixed}s"

    mult   = float(mtr_cfg.get("timeout_multiplier", 1.0))
    margin = int(mtr_cfg.get("timeout_margin_seconds", 10))
    floor_ = int(mtr_cfg.get("timeout_floor_seconds", 60))

    base = packets * interval * est_hops
    est  = int(base * max(mult, 0.1)) + max(margin, 0)
    t    = max(est, floor_)
    why  = (f"non-report: packets={packets}*interval={interval}s*est_hops={est_hops}"
            f"*mult={mult}+margin={margin}s -> {est}s; floor={floor_}s => {t}s")
    return t, why


# ---------- Normalization ----------
def _norm(h: dict) -> dict:
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


# ---------- Core runner ----------
def _build_cmd_report(ip: str, source_ip: Optional[str], settings: dict) -> list:
    mtr_cfg = settings.get("mtr", {}) or {}
    cycles   = int(mtr_cfg.get("report_cycles", 1))
    packets  = max(1, int(mtr_cfg.get("packets_per_cycle", 10)))
    interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    resolve_dns = bool(mtr_cfg.get("resolve_dns", False))

    cmd = ["mtr", "--json"]
    if not resolve_dns:
        cmd.append("-n")
    cmd += ["-c", str(packets), "-i", str(max(interval, 0.1))]
    cmd += ["--report-cycles", str(cycles)]
    if source_ip:
        try:
            fam = ipaddress.ip_address(source_ip).version
            cmd.insert(1, "-6" if fam == 6 else "-4")
        except Exception:
            pass
        cmd += ["--address", source_ip]
    cmd.append(ip)
    return cmd


def _build_cmd_nonreport(ip: str, source_ip: Optional[str], settings: dict) -> list:
    mtr_cfg = settings.get("mtr", {}) or {}
    packets  = max(1, int(mtr_cfg.get("packets_per_cycle", 10)))
    interval = float(mtr_cfg.get("per_packet_interval", 1.0))
    resolve_dns = bool(mtr_cfg.get("resolve_dns", False))

    cmd = ["mtr", "--json"]
    if not resolve_dns:
        cmd.append("-n")
    cmd += ["-c", str(packets), "-i", str(max(interval, 0.1))]
    if source_ip:
        try:
            fam = ipaddress.ip_address(source_ip).version
            cmd.insert(1, "-6" if fam == 6 else "-4")
        except Exception:
            pass
        cmd += ["--address", source_ip]
    cmd.append(ip)
    return cmd


def _run_once(cmd: list, timeout: int, logger, label: str) -> Tuple[int, str, str]:
    """Run a command in its own process group; return (rc, stdout, stderr)."""
    cmd_str = " ".join(shlex.quote(x) for x in cmd)
    if logger:
        logger.debug(f"{label} cmd: {cmd_str}")
        logger.debug(f"{label} timeout: {timeout}s")
    proc = subprocess.Popen(
        cmd,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
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
            logger.warning(f"{label} timed out after {timeout}s; cmd={cmd_str}")
        return 124, "", f"timeout after {timeout}s"
    return proc.returncode, stdout or "", stderr or ""


def _parse(stdout: str, logger, label: str) -> List[Dict[str, Any]]:
    if not stdout.lstrip().startswith("{"):
        if logger:
            snip = stdout.strip()[:400].replace("\n", "\\n")
            logger.warning(f"{label} produced non-JSON/empty output; stdout_snip={snip}")
        return []
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as je:
        if logger:
            logger.warning(f"{label} JSON parse failed: {je}; first 200 chars: {stdout[:200]!r}")
        return []
    hubs = (data.get("report") or {}).get("hubs") or []
    if not hubs:
        if logger:
            logger.warning(f"{label} JSON has no hubs.")
        return []
    return [_norm(h) for h in hubs]


def run_mtr(ip: str, source_ip: Optional[str], logger, settings: dict) -> List[Dict[str, Any]]:
    """
    Try report-cycles first (predictable, fast), otherwise fall back to non-report.
    """
    # 1) Try report-cycles
    cmd1 = _build_cmd_report(ip, source_ip, settings)
    t1, why1 = _timeout_report_mode(settings)
    if logger:
        logger.debug(f"[{ip}] timeout rationale (report-cycles) → {why1}")
    rc, out, err = _run_once(cmd1, t1, logger, f"[{ip}] mtr (report-cycles)")
    if rc == 0:
        parsed = _parse(out, logger, f"[{ip}] mtr (report-cycles)")
        if parsed:
            return parsed
        # If output was empty/invalid, continue to fallback.

    # If rc != 0 and looks like the flag is unsupported, fall back gracefully.
    unsupported = ("unrecognized option '--report-cycles'" in err.lower()
                   or "unknown option" in err.lower()
                   or "illegal option" in err.lower())
    if rc != 0 and logger:
        logger.warning(f"[{ip}] report-cycles mode failed rc={rc}; stderr={err.strip() or '(none)'}")

    # 2) Fallback: non-report, hop-aware timeout
    cmd2 = _build_cmd_nonreport(ip, source_ip, settings)
    t2, why2 = _timeout_nonreport_mode(settings)
    if logger:
        logger.debug(f"[{ip}] timeout rationale (non-report) → {why2}")
    rc2, out2, err2 = _run_once(cmd2, t2, logger, f"[{ip}] mtr (non-report)")
    if rc2 != 0:
        if logger:
            logger.warning(f"[{ip}] non-report mode failed rc={rc2}; stderr={err2.strip() or '(none)'}")
        return []
    return _parse(out2, logger, f"[{ip}] mtr (non-report)")
