# MTR_WEB — Multi-Hop Network Monitoring & Visualization

MTR_WEB is a Python-based network monitoring system that continuously measures latency, packet loss, and hop changes to multiple IP targets using [`mtr`](https://github.com/traviscross/mtr).  
It stores results in [RRDTool](https://oss.oetiker.ch/rrdtool/) databases and generates **fully static HTML dashboards** with summary and per-hop graphs, traceroutes, and recent logs.

---

## 📐 Architecture Overview

      ┌────────────────────┐
      │  mtr_targets.yaml  │
      └─────────┬──────────┘
                │
                ▼
      ┌────────────────────┐
      │   controller.py    │
      │ (watches targets & │
      │ settings, spawns   │
      │ mtr_watchdog.py)   │
      └─────────┬──────────┘
                │
    ┌───────────┴─────────────────────────┐
    │ One process per target:              │
    │ mtr_watchdog.py → monitor.py         │
    │   • Runs mtr_runner.py               │
    │   • Detects hop/loss changes         │
    │   • Updates RRD (rrd_handler.py)     │
    │   • Saves traceroute (trace_exporter)│
    └───────────┬─────────────────────────┘
                │
    ┌───────────┴──────────────────────┐
    │ graph_generator.py               │
    │   • Uses graph_jobs.py,           │
    │     graph_workers.py              │
    │   • Generates summary & per-hop   │
    │     PNG graphs from RRD           │
    └───────────┬──────────────────────┘
                │
    ┌───────────┴──────────────────────┐
    │ html_generator.py                │
    │   • target_html.py                │
    │   • per_hop_html.py               │
    │ index_generator.py                │
    │   • index_writer.py               │
    └───────────┬──────────────────────┘
                │
      ┌─────────┴──────────┐
      │   Static HTML +    │
      │    Graph Images    │
      │   (served via web) │
      └────────────────────┘


---

## 📂 Project Overview

scripts/ # Main scripts for monitoring, graphing, HTML generation, cleanup
scripts/modules/ # Core functional modules
scripts/modules/html_builder/ # Per-target and per-hop HTML builders
mtr_script_settings.yaml # Main configuration (paths, intervals, graph settings, retention)
mtr_targets.yaml # List of monitored targets
html/ # Generated static dashboard (index + per-target pages)
rrd/ # RRD databases for each target & hop
traceroute/ # Saved traceroute text & JSON hop maps
logs/ # Per-script and per-target logs


---

## ✨ Features

- **Multi-target monitoring** with one process per target, managed by `controller.py`
- **Traceroute change detection** with detailed before/after logging
- **Packet loss detection** per hop
- **RRDTool storage** with dynamic schema based on `max_hops`
- **Graph generation**:
  - Summary graphs (all hops in one)
  - Per-hop graphs
  - Multiple configurable time ranges
- **Static HTML** output:
  - `index.html` overview dashboard
  - `<ip>.html` main page per target
  - `<ip>_hops.html` per-hop detailed view
- **Configurable retention** cleanup for RRDs, graphs, logs, traceroutes, and HTML
- **Pause monitoring** for a target without deleting it (`paused: true` in `mtr_targets.yaml`)
- **Reachability check** with `fping` on dashboard (optional)

---

## ⚙️ Configuration Files

### 1. `mtr_script_settings.yaml`
Controls paths, intervals, graph parameters, RRD schema, and retention.

Example:
```yaml
log_directory: "logs"
rrd_directory: "rrd"
graph_output_directory: "html/graphs"
traceroute_directory: "traceroute"

interval_seconds: 60
max_hops: 30

graph_time_ranges:
  - label: "15m"
    seconds: 900
  - label: "1h"
    seconds: 3600
  - label: "24h"
    seconds: 86400

rrd:
  step: 60
  heartbeat: 120
  data_sources:
    - name: avg
      type: GAUGE
      min: 0
      max: U
    - name: last
      type: GAUGE
      min: 0
      max: U
    - name: best
      type: GAUGE
      min: 0
      max: U
    - name: loss
      type: GAUGE
      min: 0
      max: 100
  rras:
    - cf: AVERAGE
      xff: 0.5
      step: 1
      rows: 2016

```
### 2. `mtr_script_settings.yaml`

Example:
```yaml
targets:
  - ip: "8.8.8.8"
    description: "Google DNS"
    paused: false
  - ip: "1.1.1.1"
    description: "Cloudflare DNS"
    paused: true
```
paused: true = target is skipped without being removed from config.

```
🛠️ Installation Requirements
OS: Rocky Linux / RHEL / CentOS / Fedora / Debian / Ubuntu

Software:
Python 3.7+
mtr (network probing)
rrdtool (round-robin database)
Python bindings for RRDTool (python-rrdtool)
fping (optional, for dashboard reachability check)
Python modules: pyyaml, deepdiff

```
### Install on Rocky Linux / RHEL / CentOS

# Enable EPEL for extra packages
sudo yum install -y epel-release

# Install required system packages
sudo yum install -y mtr rrdtool python3 python3-pip python3-rrdtool fping

# Install Python dependencies
pip3 install pyyaml deepdiff


### Install on Debian / Ubuntu

sudo apt update
sudo apt install -y mtr-tiny rrdtool python3 python3-pip python3-rrdtool fping
pip3 install pyyaml deepdiff

### Usage
## Start the monitoring controller
cd /opt/scripts/MTR_WEB/scripts
python3 controller.py

## Generate graphs periodically
python3 graph_generator.py

## Generate HTML pages
python3 html_generator.py
python3 index_generator.py

## Cleanup
python3 cleanup.py

### Suggested Cron Jobs

*/2 * * * *  cd /opt/scripts/MTR_WEB/scripts && /usr/bin/python3 graph_generator.py
*/3 * * * *  cd /opt/scripts/MTR_WEB/scripts && /usr/bin/python3 html_generator.py && /usr/bin/python3 index_generator.py
7  * * * *   cd /opt/scripts/MTR_WEB/scripts && /usr/bin/python3 cleanup.py


### Example Apache Virtual

<VirtualHost *:80>
    ServerName mtr.example.com
    DocumentRoot /opt/scripts/MTR_WEB/html

    <Directory /opt/scripts/MTR_WEB/html>
        Options Indexes FollowSymLinks
        AllowOverride None
        Require all granted
    </Directory>
</VirtualHost>


### Output Structure
Logs: logs/ (per-script + per-target)
RRDs: rrd/
Graphs: html/graphs/
Traceroutes: traceroute/
HTML Pages: html/

### Screenshots

### Contributing
Pull requests are welcome.
For major changes, please open an issue first to discuss.
