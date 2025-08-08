#!/usr/bin/env python3
import json          # To parse the output from MTR (which is in JSON format)
import subprocess    # To run system commands (like MTR)

def run_mtr(target, source_ip=None, logger=None):
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
    cmd = ["mtr", "--json", "--report-cycles", "1", "--no-dns"]

    # If a source IP is provided, include it in the command
    if source_ip:
        cmd += ["--address", source_ip]

    # Add the target as the final part of the command
    cmd.append(target)

    try:
        # Run the MTR command
        result = subprocess.run(
            cmd,               # The full MTR command
            capture_output=True,  # Capture stdout and stderr
            text=True,            # Return output as a string
            timeout=20            # Give up after 20 seconds if no response
        )

        # If MTR ran successfully (exit code 0)
        if result.returncode == 0:
            return parse_mtr_output(result.stdout, logger)

        else:
            # MTR failed — log the error message
            if logger:
                logger.error(f"[MTR ERROR] {result.stderr.strip()}")
            return []

    except Exception as e:
        # Something went wrong while running the command (e.g. command not found, timeout)
        if logger:
            logger.exception(f"[EXCEPTION] MTR run failed: {e}")
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
        # Convert the JSON string into a Python dictionary
        raw = json.loads(output)

        # Get the list of hops (called "hubs" in MTR JSON)
        hops = raw["report"].get("hubs", [])

        # Annotate each hop with a "count" index and a fallback host name
        for i, hop in enumerate(hops):
            hop["count"] = i
            hop["host"] = hop.get("host", f"hop{i}")

        return hops

    except Exception as e:
        # If something fails while parsing
        if logger:
            logger.error(f"[PARSE ERROR] {e}")
        return []
