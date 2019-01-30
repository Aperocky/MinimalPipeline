import logging
import os, sys
from . import envs

# Get a logger.
def get_logger(name):
    root_ = os.environ["pipeline_path"]
    log_dir_path = envs.get_env_var("paths", "logging_dir_path")    
    log_path = os.path.join(root_, log_dir_path)
    log_file = os.path.join(log_path, "log_0.log") # This file will be bumped when full.
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Add handler and formatter
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fm = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(fm)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fm)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

    
