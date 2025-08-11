#!/usr/bin/env python3
"""
 mtr_runner.py

 Single-shot runner that executes one MTR pass against a target and returns
 a normalized list of hop dictionaries. This **compat mode** intentionally
 avoids using `--report`/`--report-cycles` because some mtr builds emit
 non-JSON when `--report` is present.

 YAML knobs (under the 'mtr' key):
   - packets_per_cycle (int): maps to -c N (packets per run)
   - per_packet_interval (float): maps to -i seconds (spacing between packets)
   - resolve_dns (bool): if false, add -n for numeric only
   - timeout_seconds (int): hard timeout for the subprocess; if 0/missing -> auto

 Function run_mtr(...) deliberately runs just one cycle. The continuous loop lives
 outside (e.g., in modules/monitor.py) so you can re-read YAML each iteration.
"""

# --- stdlib imports ---
import json            # Parse JSON text returned by 'mtr --json'
import subprocess      # Execute the 'mtr' command as a child process
import ipaddress       # Detect IPv4 vs IPv6 for a provided source IP

# Project utility to load YAML settings
from modules.utils import load_settings


def run_mtr(target, source_ip=None, logger=None, settings=None):
    """
    Run one mTR pass against 'target' and return a list of hop dicts.

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
    packets_per = int(mtr_cfg.get("packets_per_cycle", 10))       # -c N
    per_pkt_int = float(mtr_cfg.get("per_packet_interval", 1.0))  # -i seconds
    resolve_dns = bool(mtr_cfg.get("resolve_dns", False))          # add -n if False

    # Timeout logic: 0/omitted => auto. Formula: max(20, packets * interval + 5)
    yaml_timeout = int(mtr_cfg.get("timeout_seconds", 0))
    timeout_s = yaml_timeout if yaml_timeout > 0 else max(20, int(packets_per * per_pkt_int) + 5)

    # -------------------------------
    # Build the mtr command (NO --report)
    # -------------------------------
    cmd = [
        "mtr",           # executable
        "--json",        # ask for machine-readable JSON output
        "-c", str(packets_per)  # number of probes to send
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
            cmd.insert(1, "-6" if fam == 6 else "-4")      # insert right after 'mtr'
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

        # If the process exited with error, log stderr/stdout snippets and return []
        if res.returncode != 0:
            if logger:
                err_snip = res.stderr.strip()[:400].replace("
", "\n")
                out_snip = (res.stdout or "").strip()[:200].replace("
", "\n")
                logger.error(f"[MTR ERROR] rc={res.returncode} stderr={err_snip} stdout={out_snip}")
            return []

        # Precheck: ensure stdout looks like JSON before parsing
        std = (res.stdout or "").lstrip()
        if not std.startswith("{"):
            if logger:
                snip = std[:400].replace("
", "\n")
                logger.error(f"[PARSE PRECHECK] Non-JSON stdout: {snip}")
            return []

        # Parse JSON into normalized hop list
        return parse_mtr_output(std, logger)

    except Exception as e:
        # Any unexpected problem (timeout raises SubprocessError, etc.)
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
            hop["count"] = i                           # 0-based hop index for our system
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
            snippet = output[:400].replace("
", "\n")
            logger.error(f"[PARSE ERROR] {e}; stdout_snip={snippet}")
        return []
