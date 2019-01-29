import logging
import os
import utility as ut


def get_logger(name):
    root_ = os.environ["pipeline_path"]
    log_dir_path = ut.get_env_var("logging", "logging_dir_path")    
    log_path = os.path.join(root_, log_dir_path)
    log_file = os.path.join(log_path, "log_0.log") # This file will be bumped when full.

    # Create logger
    logger = logging.getLogger(name)

    # Add handler and formatter
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fm = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

    
