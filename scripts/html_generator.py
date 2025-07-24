# html_generator.py
#
# Generates one HTML page per target IP with embedded graphs, logs, and optional description.

import os
import yaml

def load_targets():
    with open("mtr_targets.yaml", "r") as f:
        return yaml.safe_load(f)['targets']

def generate_html(target, graph_dir, log_dir, output_dir):
    ip = target['ip']
    description = target.get('description', '')
    graphs = ['avg', 'loss', 'last', 'best']
    log_path = os.path.join(log_dir, f"{ip}.log")
    html_path = os.path.join(output_dir, f"{ip}.html")

    try:
        with open(log_path, "r") as log_file:
            logs = log_file.read()
    except FileNotFoundError:
        logs = "Log file not found."

    style_block = """
    <style>
        body { font-family: Arial, sans-serif; padding: 20px; background: #f9f9f9; }
        .graph { max-width: 800px; margin: 10px 0; }
        .log { background: #eee; padding: 10px; white-space: pre-wrap; max-height: 300px; overflow-y: auto; }
    </style>
    """

    with open(html_path, "w") as f:
        f.write(f"<!DOCTYPE html>\n<html>\n<head>\n<title>MTR Report for {ip}</title>\n{style_block}\n</head>\n<body>\n")
        f.write(f"<h1>MTR Report for {ip}</h1>\n")
        if description:
            f.write(f"<p><strong>Description:</strong> {description}</p>\n")
        for metric in graphs:
            f.write(f"<h3>{metric.upper()} Graph</h3>\n")
            f.write(f'<img class="graph" src="graphs/{ip}_{metric}.png" alt="{metric}">\n')
        f.write("<h3>Log</h3>\n")
        f.write(f'<div class="log">{logs}</div>\n')
        f.write("</body></html>\n")

def main():
    config = yaml.safe_load(open("mtr_script_settings.yaml"))
    targets = load_targets()
    out_dir = "html"
    os.makedirs(out_dir, exist_ok=True)
    for t in targets:
        generate_html(t, config['graph_output_directory'], config['log_directory'], out_dir)

if __name__ == "__main__":
    main()
