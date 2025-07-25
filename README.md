# MTR Monitor System

This is a modular, YAML-driven monitoring system using `mtr`, `rrdtool`, and static HTML reports. It is designed to continuously monitor multiple targets, record per-hop metrics, and present them in an HTML dashboard.

---

## 📁 Directory Structure

```
mtr_monitor/
├── scripts/
│   ├── mtr_monitor.py         # Main monitor script per target
│   ├── controller.py          # Starts/stops monitor processes based on YAML config
│   ├── graph_generator.py     # Generates graphs from RRD files
│   ├── html_generator.py      # Creates a per-target HTML summary page
│   └── index_generator.py     # Creates the dashboard linking all target pages
├── mtr_targets.yaml           # List of monitored IPs and optional source IPs
├── mtr_script_settings.yaml   # Global settings (paths, interval, retention, etc.)
├── html/
│   ├── index.html             # Landing dashboard (auto-generated)
│   ├── <ip>.html              # Per-target HTML pages
│   └── graphs/                # RRD-generated PNGs
├── data/                      # RRD files for each monitored IP
└── logs/                      # Per-target monitoring logs
```

---

## ⚙️ Configuration

### `mtr_targets.yaml`

```yaml
targets:
  - ip: 8.8.8.8
    source_ip: 192.168.1.10
    description: Google Public DNS

  - ip: 1.1.1.1
    source_ip: null
    description: Cloudflare DNS

mtr_settings:
  count: 10
  max_hops: 30
```

### `mtr_script_settings.yaml`

```yaml
interval_seconds: 60
log_directory: logs/
rrd_directory: data/
graph_output_directory: html/graphs/
retention_days: 30
max_hops: 30
```

---

## 🚀 How to Use

### 1. Start Monitoring Controller

```bash
python3 scripts/controller.py
```

This watches `mtr_targets.yaml` and automatically starts/stops per-target monitors.

### 2. Generate Graphs Periodically

```bash
python3 scripts/graph_generator.py
```

You can automate it with a cronjob:

```cron
*/5 * * * * /usr/bin/python3 /path/to/scripts/graph_generator.py
```

### 3. Generate HTML Reports

```bash
python3 scripts/html_generator.py
python3 scripts/index_generator.py
```

These create `html/<ip>.html` and `html/index.html`.

---

## 🧪 Log Monitoring

Each target has a dedicated log:
```
logs/8.8.8.8.log
```

Logs include:
- Script start/stop
- Hop changes
- Packet loss detections

---

## 🌐 HTML Dashboard

- Open `html/index.html` in a browser
- Each target has:
  - Graphs for `avg`, `last`, `best`, `loss`
  - Embedded logs
  - Status summary (last seen, reachability)

---

## 🧹 Retention

Set in `mtr_script_settings.yaml` as `retention_days`. You can create a cleanup script to delete old files based on timestamps or file age.

---

## 🔧 Requirements

- `python3`
- `rrdtool` + Python bindings
- `mtr` with JSON support
- `yaml` (PyYAML)

Install Python packages:
```bash
pip3 install pyyaml
```

---

## 📬 Contact

Maintained by: [Pizu]
