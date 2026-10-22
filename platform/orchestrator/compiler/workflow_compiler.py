
import yaml

def compile_workflow(yaml_path: str):
    with open(yaml_path, 'r') as f:
        workflow = yaml.safe_load(f)
    return workflow
