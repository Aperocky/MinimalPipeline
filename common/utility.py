import os, sys
import shutil
import configparser

# Helper function dealing with file systems and others.

def remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)
        
def get_env_var(section, specific):
    project_dir = os.environ["pipeline_path"]
    envs_path = os.path.join(project_dir, 'environment/envs.ini')
    envs = configparser.configParser()
    envs.read(envs_path)
    return envs.get(section, specific)
  

