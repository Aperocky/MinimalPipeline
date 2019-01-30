# Load env files. This is a standalone file as it cannot import anything else from project to avoid cyclical dependency.
import os
import configparser

def get_env_var(section, specific):
    project_dir = os.environ["pipeline_path"]
    envs_path = os.path.join(project_dir, 'environment/envs.ini')
    envs = configparser.ConfigParser()
    envs.read(envs_path)
    return envs.get(section, specific)
 
