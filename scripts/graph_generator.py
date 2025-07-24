# graph_generator.py
#
# Generates graphs for all metrics per target from RRD files.
# Intended to run periodically via cron.

import rrdtool
import os
import yaml

def load_targets(filename):
    with open(filename, 'r') as f:
        return yaml.safe_load(f)['targets']

def generate_graphs(ip, max_hops, rrd_dir, graph_dir):
    os.makedirs(graph_dir, exist_ok=True)
    metrics = ['avg', 'loss', 'last', 'best']
    for metric in metrics:
        defs = []
        lines = []
        for i in range(1, max_hops + 1):
            ds_name = f"hop{i}_{metric}"
            defs.append(f"DEF:h{i}={rrd_dir}/{ip}.rrd:{ds_name}:AVERAGE")
            lines.append(f"LINE1:h{i}#%06X:Hop {i}" % (0xFF0000 + (i * 3000) % 0xFFFFFF))
        output = f"{graph_dir}/{ip}_{metric}.png"
        rrdtool.graph(output,
                      "--start", "-1h",
                      "--title", f"{metric.capitalize()} to {ip}",
                      "--width", "800", "--height", "200",
                      *defs,
                      *lines)

def main():
    settings = yaml.safe_load(open("mtr_script_settings.yaml"))
    targets = load_targets("mtr_targets.yaml")
    rrd_dir = settings['rrd_directory']
    graph_dir = settings['graph_output_directory']
    max_hops = settings['max_hops']
    for t in targets:
        generate_graphs(t['ip'], max_hops, rrd_dir, graph_dir)

if __name__ == "__main__":
    main()
