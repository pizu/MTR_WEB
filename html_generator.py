# html_generator.py
#
# Generates one HTML page per target IP with embedded graphs, logs (latest X lines), and optional description.

import os
import yaml

def load_targets():
    with open("mtr_targets.yaml", "r") as f:
        return yaml.safe_load(f)['targets']

def read_last_lines(log_path, max_lines):
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
        if not lines:
            return "No log data yet."
        return "".join(reversed(lines[-max_lines:]))
    except FileNotFoundError:
        return "Log file not found."

def generate_html(target, graph_dir, log_dir, output_dir, log_lines):
    ip = target['ip']
    description = target.get('description', '')
    graphs = ['avg', 'loss', 'last', 'best']
    log_content = read_last_lines(os.path.join(log_dir, f"{ip}.log"), log_lines)

    html_path = os.path.join(output_dir, f"{ip}.html")
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
        f.write("<h3>Log (most recent at top)</h3>\n")
        f.write(f'<div class="log">{log_content}</div>\n')
        f.write("</body></html>\n")

def main():
    config = yaml.safe_load(open("mtr_script_settings.yaml"))
    targets = load_targets()
    out_dir = "html"
    os.makedirs(out_dir, exist_ok=True)
    for t in targets:
        generate_html(t, config['graph_output_directory'], config['log_directory'], out_dir, config.get("log_lines_to_show", 100))

if __name__ == "__main__":
    main()
