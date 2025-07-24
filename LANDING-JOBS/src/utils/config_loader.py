from jinja2 import Template
import yaml

def load_config_pipeline(config_name, context_vars=None):
    """
    Load cấu hình từ file YAML trong zip.
    Nếu chạy ở mode 'cluster', dùng os.getcwd().
    """


    with open(config_name, 'r') as file:
        raw_content = file.read()
    
    if context_vars:
        template = Template(raw_content)
        rendered_content = template.render(**context_vars)
    else:
        rendered_content = raw_content

    config = yaml.safe_load(rendered_content)
    return config