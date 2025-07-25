# MTR_WEB Monitoring Suite

MTR_WEB is a Python-based monitoring system that continuously runs MTR (`mtr --json`) for a set of public IPs. It collects hop metrics (loss, latency, etc.), stores them in RRD files, and generates dynamic HTML dashboards and daily logs for network path analysis.

---

## 📦 Project Structure

```
MTR_WEB/
├── html/                       # HTML output for dashboards
├── scripts/
│   ├── controller.py          # Main orchestrator: manages mtr_monitor.py for each IP
│   ├── mtr_monitor.py         # Collects and logs MTR data, updates RRDs
│   ├── graph_generator.py     # Creates graphs from RRD files
│   ├── html_generator.py      # Generates per-target HTML pages
│   ├── index_generator.py     # Generates master index page
│   └── utils.py               # Shared settings/logger utilities
├── mtr_targets.yaml           # List of monitored targets and descriptions
├── mtr_script_settings.yaml   # Global settings: intervals, paths, thresholds
└── README.md
```

---

## 🚀 Installation

1. **Install dependencies** (Python 3.8+ recommended):

```bash
sudo apt update
sudo apt install python3 python3-pip fping mtr rrdtool
pip3 install deepdiff pyyaml
```

2. **Clone this repo** and prepare the directory structure:

```bash
git clone https://github.com/your-org/MTR_WEB.git
cd MTR_WEB
mkdir -p html rrd logs traceroute
```

3. **Edit configuration files**:
- `mtr_targets.yaml`
- `mtr_script_settings.yaml`

---

## ⚙️ Configuration

### `mtr_targets.yaml`

This file defines the list of IPs to monitor and optional settings per target.

```yaml
targets:
  - ip: 8.8.8.8
    source_ip: null              # Optional: use specific source interface
    description: Google DNS

  - ip: 1.1.1.1
    source_ip: 192.168.1.10      # Optional source IP
    description: Cloudflare
```

### `mtr_script_settings.yaml`

This is the global settings file for intervals, paths, and thresholds.

```yaml
log_directory: logs
rrd_directory: rrd
traceroute_directory: traceroute
interval_seconds: 60
max_hops: 30
loss_threshold: 10              # % packet loss to trigger alert
enable_fping_check: true
retention_days: 30              # Optional: for log/graph cleanup
html_log_lines: 100             # Show latest X log lines in HTML
```

---

## ▶️ How to Run

Start the controller to monitor and manage processes:

```bash
python3 scripts/controller.py
```

This will:
- Watch `mtr_targets.yaml` for changes
- Start/stop `mtr_monitor.py` per IP
- Collect MTR stats and update RRDs
- Generate HTML and logs automatically

You can also generate components manually:

```bash
python3 scripts/graph_generator.py
python3 scripts/html_generator.py
python3 scripts/index_generator.py
```

---

## 📊 Example Outputs

### HTML

Open `html/index.html` in your browser to view:

- A table of all monitored targets
- Per-IP pages with:
  - Traceroute hop list
  - RRD graphs (loss, avg latency, best/worst)
  - Description and last seen status
  - Recent logs (packet loss, hop changes)

### Logs

Each script writes to its own file in the `logs/` directory.

Example: `logs/mtr_monitor.log`

```text
2025-07-25 19:20:12,432 [INFO] Started monitoring 8.8.8.8
2025-07-25 19:21:13,210 [WARNING] 8.8.8.8 - Hop 4 (203.0.113.5) increased loss: 15%
2025-07-25 19:22:05,872 [INFO] Hop path changed (diff):
- 4: 203.0.113.5
+ 4: 198.51.100.23
```

---

## ⏱️ Cron Job Examples

To automate reporting and cleanup tasks, you can schedule them using cron.

### Daily HTML Refresh at 7 AM:
```
0 7 * * * /usr/bin/python3 /opt/scripts/MTR_WEB/scripts/html_generator.py
0 7 * * * /usr/bin/python3 /opt/scripts/MTR_WEB/scripts/index_generator.py
```

### Daily RRD Graph Generation:
```
*/10 * * * * /usr/bin/python3 /opt/scripts/MTR_WEB/scripts/graph_generator.py
```

### Optional: Daily Log or Data Cleanup (if implemented):
```
30 1 * * * /usr/bin/python3 /opt/scripts/MTR_WEB/scripts/cleanup.py
```

---

## 📈 RRD Graph Details

Each target has a single RRD file that includes all hop metrics:

- **File format**: `rrd/<target_ip>.rrd`
- **Data Sources (DS)**:
  - hop{N}_loss (packet loss %)
  - hop{N}_avg (average latency)
  - hop{N}_last (last recorded latency)
  - hop{N}_best (lowest latency)

### Example RRDTool command to inspect a file:

```bash
rrdtool info rrd/8.8.8.8.rrd
```

### Example: Graph for loss over the last 12 hours

```bash
rrdtool graph loss.png \
  --start -43200 --end now \
  --title "Packet Loss - 8.8.8.8" \
  DEF:hop0=rrd/8.8.8.8.rrd:hop0_loss:AVERAGE \
  LINE2:hop0#FF0000:"Hop 0"
```

Use `graph_generator.py` to automate graph creation for all hops and time ranges.

---

## 🛠 Author & License

Developed by [Pizu]  
License: MIT

