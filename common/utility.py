import os, sys
import shutil
import configparser
from . import logs

logger = logs.get_logger(__name__)

# Helper function dealing with file systems and others.

def remove_path(path, project=True):
    if project:
        root_path = os.environ["pipeline_path"]
        path = os.path.join(root_path, path)
    if os.path.isdir(path):
        logger.info("Removing directory {} recursively".format(path))
        shutil.rmtree(path)
    elif os.path.isfile(path):
        logger.info("Removing file {}".format(path))
        os.remove(path)
    else:
        logger.warning("{} does not exist".format(path))
        
def get_env_var(section, specific):
    project_dir = os.environ["pipeline_path"]
    envs_path = os.path.join(project_dir, 'environment/envs.ini')
    envs = configparser.configParser()
    envs.read(envs_path)
    return envs.get(section, specific)
  

