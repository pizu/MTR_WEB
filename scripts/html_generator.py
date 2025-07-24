# html_generator.py
#
# Generates one HTML page per target IP with embedded graphs and log content.
# Places them in the html/ directory with filename <target>.html.

import os
import yaml

def load_targets():
    with open("mtr_targets.yaml", "r") as f:
        return yaml.safe_load(f)['targets']

def generate_html(target_ip, graph_dir, log_dir, output_dir):
    graphs = ['avg', 'loss', 'last', 'best']
    log_path = os.path.join(log_dir, f"{target_ip}.log")
    html_path = os.path.join(output_dir, f"{target_ip}.html")

    try:
        with open(log_path, "r") as log_file:
            logs = log_file.read()
    except FileNotFoundError:
        logs = "Log file not found."

    with open(html_path, "w") as f:
        f.write(f'''<!DOCTYPE html>
<html>
<head>
    <title>MTR Report for {target_ip}</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
        .graph {{ max-width: 800px; margin: 10px 0; }}
        .log {{ background: #eee; padding: 10px; white-space: pre-wrap; max-height: 300px; overflow-y: auto; }}
    </style>
</head>
<body>
    <h1>MTR Report for {target_ip}</h1>
''')
        for metric in graphs:
            f.write(f'<h3>{metric.upper()} Graph</h3>\n')
            f.write(f'<img class="graph" src="graphs/{target_ip}_{metric}.png" alt="{metric}">\n')
        f.write("<h3>Log</h3>\n")
        f.write(f"<div class=\"log\">{logs}</div>\n")
        f.write("</body></html>\n")

def main():
    config = yaml.safe_load(open("mtr_script_settings.yaml"))
    targets = load_targets()
    out_dir = "html"
    os.makedirs(out_dir, exist_ok=True)
    for t in targets:
        generate_html(t['ip'], config['graph_output_directory'], config['log_directory'], out_dir)

if __name__ == "__main__":
    main()
