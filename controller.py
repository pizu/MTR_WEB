# controller.py
#
# This script watches mtr_targets.yaml and manages child mtr_monitor.py processes per target.
# When a target is added or removed, it starts or stops the monitor accordingly.
#
# Usage: python3 controller.py
# Ensure mtr_monitor.py and the YAML files are in place.

import yaml
import time
import subprocess
import os
import signal
import threading

CONFIG_FILE = "mtr_targets.yaml"
SCRIPT_SETTINGS_FILE = "mtr_script_settings.yaml"
MONITOR_SCRIPT = "scripts/mtr_monitor.py"

# Track running processes per target
monitored_targets = {}
lock = threading.Lock()

# Load YAML config
def load_targets():
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)['targets']

# Load script settings
def load_settings():
    with open(SCRIPT_SETTINGS_FILE, 'r') as f:
        return yaml.safe_load(f)

# Start a target monitor
def start_monitor(target, settings):
    args = [
        "python3", MONITOR_SCRIPT,
        "--target", target['ip'],
        "--settings", SCRIPT_SETTINGS_FILE
    ]
    if target.get("source_ip"):
        args += ["--source", target["source_ip"]]
    proc = subprocess.Popen(args)
    return proc

# Stop all monitors
def stop_all():
    with lock:
        for ip, proc in monitored_targets.items():
            proc.terminate()
        monitored_targets.clear()

# Watch the config file and manage targets
def monitor_loop():
    settings = load_settings()
    current_targets = []

    while True:
        try:
            new_targets = load_targets()
            new_ips = {t['ip'] for t in new_targets}
            cur_ips = set(monitored_targets.keys())

            # Start new targets
            for target in new_targets:
                if target['ip'] not in cur_ips:
                    print(f"Starting monitor for {target['ip']}")
                    proc = start_monitor(target, settings)
                    monitored_targets[target['ip']] = proc

            # Stop removed targets
            for ip in cur_ips - new_ips:
                print(f"Stopping monitor for {ip}")
                proc = monitored_targets.pop(ip)
                proc.terminate()

            time.sleep(10)
        except KeyboardInterrupt:
            stop_all()
            break
        except Exception as e:
            print(f"Error in controller: {e}")
            time.sleep(10)

if __name__ == "__main__":
    monitor_loop()
