#!/usr/bin/env python3
"""
mtr_runner.py

Single-shot runner that executes ONE MTR pass and returns a normalized list of
hop dictionaries. This **compat mode** deliberately avoids `--report` because
some mtr builds output non-JSON when `--report` is present.

YAML knobs (mtr_script_settings.yaml -> mtr.*):
  - packets_per_cycle (int)      -> -c N      (probes to send in this pass)
  - per_packet_interval (float)  -> -i secs   (spacing between probes)
  - resolve_dns (bool)           -> -n        (when False, keep numeric only)
  - timeout_seconds (int)        -> hard timeout for subprocess
                                    (0/missing -> auto: max(20, packets*interval + 5))
NOTE: If you still have mtr.report_cycles in YAML, it is ignored in this mode.
"""

# --- stdlib imports ---
import json            # parse JSON text from 'mtr --json'
import subprocess      # run the 'mtr' command
import ipaddress       # detect IPv4 vs IPv6 when a source IP is provided

# --- project imports ---
from modules.utils import load_settings  # read YAML settings


def run_mtr(target, source_ip=None, logger=None, settings=None):
    """
    Run a single MTR pass to 'target' and return a list[dict] of hops.

    Args:
        target (str): Destination host/IP to probe.
        source_ip (str|None): Optional source address (passed via --address).
        logger (logging.Logger|None): Optional logger for debug/error messages.
        settings (dict|None): Optional pre-loaded YAML settings (saves a file read).

    Returns:
        list[dict]: normalized hops with numeric fields as floats; [] on error.
    """
    # Load YAML if caller didn't pass a settings dict.
    if settings is None:
        settings = load_settings("mtr_script_settings.yaml")

    # Pull the 'mtr' subsection; default to {} if missing.
    mtr_cfg = settings.get("mtr", {})

    # Read knobs with safe defaults (so missing keys don't break the run).
    packets_per = int(mtr_cfg.get("packets_per_cycle", 10))       # -c N
    per_pkt_int = float(mtr_cfg.get("per_packet_interval", 1.0))  # -i seconds
    resolve_dns = bool(mtr_cfg.get("resolve_dns", False))         # add -n if False

    # Compute timeout:
    #  - If YAML value > 0, use it.
    #  - Else auto: enough time for packets*interval, plus margin, min 20s.
    yaml_timeout = int(mtr_cfg.get("timeout_seconds", 0))
    timeout_s = yaml_timeout if yaml_timeout > 0 else max(20, int(packets_per * per_pkt_int) + 5)

    # Build the mtr command (NO --report, to keep JSON reliable).
    cmd = [
        "mtr",               # executable
        "--json",            # machine-readable output
        "-c", str(packets_per)  # how many probes to send
    ]

    # Numeric hostnames (faster, less noisy logs) if resolve_dns is False.
    if not resolve_dns:
        cmd.append("-n")

    # Non-default per-packet interval? Add -i.
    if per_pkt_int != 1.0:
        cmd += ["-i", str(per_pkt_int)]

    # If a source is provided, force family with -4/-6 and pass --address.
    if source_ip:
        try:
            fam = ipaddress.ip_address(source_ip).version  # 4 or 6
            cmd.insert(1, "-6" if fam == 6 else "-4")      # insert right after "mtr"
        except Exception:
            # If detection fails, skip forcing; mtr will choose based on target.
            pass
        cmd += ["--address", source_ip]

    # Append the destination target last.
    cmd.append(str(target))

    # Helpful for reproducing issues: log the exact command.
    if logger:
        logger.debug(f"MTR cmd: {' '.join(cmd)}")

    try:
        # Run the command, capture stdout/stderr as text, with a hard timeout.
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)

        # Non-zero exit? Log stderr/stdout snippets and return [] so caller can handle it.
        if res.returncode != 0:
            if logger:
                err_snip = res.stderr.strip()[:400].replace("\n", "\\n")
                out_snip = (res.stdout or "").strip()[:200].replace("\n", "\\n")
                logger.error(f"[MTR ERROR] rc={res.returncode} stderr={err_snip} stdout={out_snip}")
            return []

        # Quick precheck: real JSON starts with '{'. If not, log a snippet and bail.
        std = (res.stdout or "").lstrip()
        if not std.startswith("{"):
            if logger:
                snip = std[:400].replace("\n", "\\n")
                logger.error(f"[PARSE PRECHECK] Non-JSON stdout: {snip}")
            return []

        # Parse JSON and normalize hop metrics.
        return parse_mtr_output(std, logger)

    except Exception as e:
        # Timeout or any other unexpected exception: log and return [].
        if logger:
            logger.exception(f"[EXCEPTION] MTR run failed: {e}")
        return []


def parse_mtr_output(output, logger=None):
    """
    Convert raw 'mtr --json' output into a normalized hop list.

    Normalization:
      - add 0-based 'count' (stable index for our RRD/HTML)
      - ensure 'host' exists (fallback to 'hop<N>')
      - coerce 'Loss%', 'Avg', 'Best', 'Last' to float (0.0 if missing/invalid)
    """
    try:
        raw = json.loads(output)  # may raise ValueError on bad JSON

        # mtr emits {"report": {"mtr": {...}, "hubs": [ ... ]}}
        hops = raw.get("report", {}).get("hubs", [])

        for i, hop in enumerate(hops):
            hop["count"] = i                          # 0-based hop index for our system
            hop["host"] = hop.get("host", f"hop{i}")  # fallback if host is missing

            # Convert commonly-used numeric fields to floats for RRD/math.
            for k in ("Loss%", "Avg", "Best", "Last"):
                try:
                    hop[k] = float(hop.get(k, 0))
                except Exception:
                    hop[k] = 0.0

        return hops

    except Exception as e:
        # Include a short snippet of the offending output for triage.
        if logger:
            snippet = output[:400].replace("\n", "\\n")
            logger.error(f"[PARSE ERROR] {e}; stdout_snip={snippet}")
        return []
