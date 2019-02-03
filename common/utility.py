import os, sys
import shutil
import configparser
import uuid
from . import logs, envs

logger = logs.get_logger(__name__)
root_path = os.environ["pipeline_path"]

# Helper function dealing with file systems and others.

# This function removes a certain path recursively.
def remove_path(path, project=True):
    if project:
        path = os.path.join(root_path, path)
    if os.path.isdir(path):
        logger.info("Removing directory {} recursively".format(path))
        shutil.rmtree(path)
    elif os.path.isfile(path):
        logger.info("Removing file {}".format(path))
        os.remove(path)
    else:
        logger.warning("{} does not exist".format(path))


# This function creates an empty directory.
def create_dir(path, project=True):
    if project:
        path = os.path.join(root_path, path)
    if os.path.exists(path):
        logger.error("File already exists")
        raise FileExistsError("File already exists")
    else:
        logger.info("Creating empty directory {}".format(path))
        os.mkdir(path)
        

# Creates random temp directory to process tasks in isolated environments.
def create_temp_dir(path=""):
    if not path:
        path = envs.get_env_var("paths", "temp_dir_path")
    temp_path = uuid.uuid4()
    path = os.path.join(root_path, path, temp_path)
    logger.info("Creating temp directory {}".format(path))
    create_dir(path)
    return path

def copy_directory(from_path, to_path, overwrite=False):
    if not os.path.isdir(from_path):
        logger.error("Copied directory does not exist")
        raise FileNotFoundError("Source directory does not exist")
    if os.path.isdir(to_path):
        if not overwrite:
            logger.error("Destination directory already exist")
            raise FileExistsError("Destination exists")
        else:
            remove_path(to_path)
    logger.info("Copying directory {} to {}".format(from_path, to_path))
    shutil.copytree(from_path, to_path)

