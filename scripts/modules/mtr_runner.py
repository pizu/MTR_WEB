#!/usr/bin/env python3
"""
 mtr_runner.py

 Single-shot runner that executes one MTR report against a target and returns
 a normalized list of hop dictionaries. It reads knobs from mtr_script_settings.yaml.

 YAML knobs (under the 'mtr' key):
   - report_cycles (int): maps to --report-cycles N (how many snapshots per run)
   - packets_per_cycle (int): maps to -c N (packets per snapshot)
   - resolve_dns (bool): if false, add -n for numeric only
   - per_packet_interval (float): maps to -i seconds (spacing between packets)
   - timeout_seconds (int): hard timeout for the subprocess; if 0/missing -> auto

 Function run_mtr(...) deliberately runs just one cycle. The continuous loop lives
 outside (e.g., in modules/monitor.py) so you can re-read YAML each iteration.
"""

# Standard library imports used by the runner
import json            # to parse the JSON output returned by 'mtr --json'
import subprocess      # to execute the 'mtr' command as a child process
import ipaddress       # to detect IPv4 vs IPv6 for the provided source IP

# Project utility to load YAML settings
from modules.utils import load_settings


def run_mtr(target, source_ip=None, logger=None, settings=None):
    """
    Run one MTR report against 'target' and return a list of hop dicts.

    Args:
        target (str): Destination host/IP to probe.
        source_ip (str|None): Optional source address; passed to mtr via --address.
        logger (logging.Logger|None): Optional logger for debug/error messages.
        settings (dict|None): Optional pre-loaded settings (saves a file read).

    Returns:
        list[dict]: Hops with normalized numeric fields; [] on error.
    """

    # If the caller didn't pass a settings dict, read the default YAML file now.
    if settings is None:
        settings = load_settings("mtr_script_settings.yaml")

    # Pull the 'mtr' subsection from YAML; default to {} if missing
    mtr_cfg = settings.get("mtr", {})

    # Read each knob with sane defaults so missing YAML keys don't break the run
    report_cycles = int(mtr_cfg.get("report_cycles", 1))            # --report-cycles
    packets_per   = int(mtr_cfg.get("packets_per_cycle", 10))       # -c
    resolve_dns   = bool(mtr_cfg.get("resolve_dns", False))          # add -n if False
    per_pkt_int   = float(mtr_cfg.get("per_packet_interval", 1.0))   # -i seconds

    # Timeout logic: if YAML gives 0 or omits it, compute a safe automatic timeout
    # Formula: max(20, cycles * packets * interval + 5) to cover slow/long paths
    yaml_timeout  = int(mtr_cfg.get("timeout_seconds", 0))
    if yaml_timeout > 0:
        timeout_s = yaml_timeout
    else:
        timeout_s = max(20, int(report_cycles * packets_per * per_pkt_int) + 5)

    # Build the base mtr command with JSON output and a fixed report mode
    cmd = [
        "mtr",           # executable
        "--json",        # ask for machine-readable JSON output
        "--report",      # run in report mode (non-interactive, returns a summary)
        "--report-cycles", str(report_cycles),  # how many snapshots to include
        "-c", str(packets_per)                  # packets per snapshot
    ]

    # If we do NOT want DNS resolution, append '-n' to keep addresses numeric
    if not resolve_dns:
        cmd.append("-n")

    # If the user changed the per-packet interval from the default 1.0s, include it
    if per_pkt_int != 1.0:
        cmd += ["-i", str(per_pkt_int)]

    # If a source address is provided, force the IP family and pass --address
    if source_ip:
        try:
            fam = ipaddress.ip_address(source_ip).version  # 4 or 6
            cmd.insert(0, "-6" if fam == 6 else "-4")      # preprend -4/-6 for clarity
        except Exception:
            # If detection fails, skip forcing the family; mtr will try to pick
            pass
        cmd += ["--address", source_ip]

    # Finally append the target host/IP to probe
    cmd.append(str(target))

    # Log the full command for reproducibility when debugging
    if logger:
        logger.debug(f"MTR cmd: {' '.join(cmd)}")

    try:
        # Execute the command, capture stdout/stderr as text, enforce a timeout
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)

        # If the process exited cleanly and produced some output, parse it
        if res.returncode == 0 and res.stdout.strip():
            return parse_mtr_output(res.stdout, logger)

        # Otherwise, log the error code and any stderr text to help diagnose
        if logger:
            logger.error(f"[MTR ERROR] rc={res.returncode} msg={res.stderr.strip()}")
        return []  # return an empty hop list on failure so callers can handle it

    except Exception as e:
        # Any unexpected problem (timeout raises SubprocessError, JSON, etc.)
        if logger:
            logger.exception(f"[EXCEPTION] MTR run failed: {e}")
        return []


def parse_mtr_output(output, logger=None):
    """
    Convert raw JSON output from 'mtr --json' into a normalized hop list.

    - Ensures keys exist for 'host' and adds a 0-based 'count' per hop.
    - Forces numeric fields to float: 'Loss%', 'Avg', 'Best', 'Last'.
    - Returns [] if the payload is malformed or missing the expected structure.
    """
    try:
        # Parse the JSON string into a Python dict
        raw = json.loads(output)

        # mtr's JSON report has a top-level 'report' object with a 'hubs' array
        hops = raw.get("report", {}).get("hubs", [])

        # Normalize every hop entry for downstream code (RRD, HTML, logging)
        for i, hop in enumerate(hops):
            hop["count"] = i                           # 0-based hop index
            hop["host"] = hop.get("host", f"hop{i}")  # fallback label if missing

            # Convert known numeric fields to float; default to 0.0 on errors
            for k in ("Loss%", "Avg", "Best", "Last"):
                try:
                    hop[k] = float(hop.get(k, 0))
                except Exception:
                    hop[k] = 0.0

        return hops

    except Exception as e:
        # If JSON can't be parsed or structure is unexpected, log and return []
        if logger:
            logger.error(f"[PARSE ERROR] {e}")
        return []
