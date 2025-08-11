#!/usr/bin/env python3
import ipaddress
import json          # To parse the output from MTR (which is in JSON format)
import subprocess    # To run system commands (like MTR)
from modules.utils import load_settings

def run_mtr(target, source_ip=None, logger=None, settings=None):
    """
    This function runs the MTR (My Traceroute) command for a single target.

    Parameters:
    - target: The destination IP or hostname you want to trace.
    - source_ip: If you want MTR to send from a specific interface or IP, provide it here.
    - logger: Used to log errors or information if something goes wrong.

    Returns:
    - A list of dictionaries, each representing a hop (router) on the path to the target.
    """

    # Build the base MTR command
    if settings is None:
        settings = load_settings("mtr_script_settings.yaml")

    mtr_cfg = settings.get("mtr", {})
    report_cycles = int(mtr_cfg.get("report_cycles", 1))
    packets_per   = int(mtr_cfg.get("packets_per_cycle", 10))
    resolve_dns   = bool(mtr_cfg.get("resolve_dns", False))
    per_pkt_int   = float(mtr_cfg.get("per_packet_interval", 1.0))  # optional
    timeout_s     = int(mtr_cfg.get(
        "timeout_seconds",
        max(20, int(report_cycles * packets_per * per_pkt_int) + 5)
    ))

    cmd = ["mtr", "--json", "--report", "--report-cycles", str(report_cycles), "-c", str(packets_per)]
    if not resolve_dns:
        cmd.append("-n")
    if per_pkt_int != 1.0:
        cmd += ["-i", str(per_pkt_int)]
    if source_ip:
        try:
            fam = ipaddress.ip_address(source_ip).version
            cmd.insert(0, "-6" if fam == 6 else "-4")
        except Exception:
            pass
        cmd += ["--address", source_ip]
    cmd.append(str(target))
    if logger: logger.debug(f"MTR cmd: {' '.join(cmd)}")

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        if res.returncode == 0 and res.stdout.strip():
            return parse_mtr_output(res.stdout, logger)
        if logger: logger.error(f"[MTR ERROR] rc={res.returncode} msg={res.stderr.strip()}")
        return []
    except Exception as e:
        if logger: logger.exception(f"[EXCEPTION] MTR run failed: {e}")
        return []


def parse_mtr_output(output, logger=None):
    """
    This function parses the output of the MTR command (which is in JSON format).

    It returns a list of hop entries, where each entry contains:
    - Loss% : Packet loss percentage
    - Avg   : Average latency
    - Last  : Most recent latency
    - Best  : Lowest latency
    - host  : IP address or hostname

    Parameters:
    - output: The JSON string from MTR
    - logger: Logger for error handling

    Returns:
    - List of dictionaries, one per hop
    """

    try:
        raw = json.loads(output)
        hops = raw.get("report", {}).get("hubs", [])
        for i, hop in enumerate(hops):
            hop["count"] = i
            hop["host"] = hop.get("host", f"hop{i}")
            for k in ("Loss%", "Avg", "Best", "Last"):
                try: hop[k] = float(hop.get(k, 0))
                except: hop[k] = 0.0
        return hops
    except Exception as e:
        if logger: logger.error(f"[PARSE ERROR] {e}")
        return []
