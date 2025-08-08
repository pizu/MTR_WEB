#!/usr/bin/env python3
import os
import json

def save_trace_and_json(ip, hops, settings, logger):
    """
    Saves traceroute results for a target in two formats:

    1. <target>.trace.txt — A plain text file listing each hop with its average latency
    2. <target>.json      — A JSON file mapping hop numbers to host/IP

    Parameters:
    - ip: The IP address or hostname of the monitored target
    - hops: A list of hop dictionaries returned by parse_mtr_output()
    - settings: Configuration from mtr_script_settings.yaml
    - logger: Logger instance for logging status messages
    """

    # Get directory where trace files will be saved (default: "traceroute")
    traceroute_dir = settings.get("traceroute_directory", "traceroute")

    # Create the directory if it does not exist
    os.makedirs(traceroute_dir, exist_ok=True)

    # ---------------------------------------
    # Save human-readable traceroute to .txt
    # ---------------------------------------
    txt_path = os.path.join(traceroute_dir, f"{ip}.trace.txt")

    with open(txt_path, "w") as f:
        for hop in hops:
            hop_num = hop.get("count", "?")       # Hop number (0, 1, 2, etc.)
            ip_addr = hop.get("host", "?")        # IP or hostname
            latency = hop.get("Avg", "U")         # Average latency at this hop
            f.write(f"{hop_num} {ip_addr} {latency} ms\n")  # Write line to file

    logger.info(f"Saved traceroute to {txt_path}")

    # ---------------------------------------
    # Save hop map to .json file
    # ---------------------------------------
    json_path = os.path.join(traceroute_dir, f"{ip}.json")

    # Create a dictionary like: {"hop0": "192.168.1.1", "hop1": "10.0.0.1", ...}
    hop_map = {
        f"hop{hop['count']}": hop.get("host", f"hop{hop['count']}")
        for hop in hops
    }

    # Save the dictionary as JSON
    with open(json_path, "w") as f:
        json.dump(hop_map, f, indent=2)

    logger.info(f"Saved hop label map to {json_path}")
