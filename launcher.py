import configparser as cfg
import importlib
import sys
from common.repository import Repository
from common.task import Task


class Launcher:

    def __init__(self, config_path):
        self.config_path = config_path
        self.config = None
        self.repos = {}
        self.task_instance = None
        self.initiate_task()
        self.initiate_repository(self)

    # Return a task instance of the task being executed.
    def initiate_task(self):
        config = cfg.ConfigParser()
        config.read(self.config_path)
        self.config = config
        module_path = config.get("source_path", "module") # common.repository
        class_name = config.get("source_path", "class") # "Repository"
        task_module = importlib.import_module(module_path)
        task_class = getattr(task_module, class_name)
        self.task_instance = task_class()

    # Fill the repository (paths) for the task to be executed
    def initiate_repository(self):
        self.repos["upstream"] = {}
        try:
            for section in config.sections():
                if section.startswith("input"):
                    upstream = config.get(section, "repository_path")
                    numblocks = int(config.get(section, "blocks"))
                    self.repos["upstream"][upstream] = {}
                    streamrepo = self.repos["upstream"][upstream]
                    streamrepo["repository"] = Repository(upstream)
                    streamrepo["blocks"] = numblocks
            local_path = config.get("output", "repository_path")
            self.repos["local"] = Repository(local_path)
        except BlockingIOError as e:
            logger.warning("Not all repository are readable, awaiting next execution")
            sys.exit()

        
    def run(self):
        # TODO: running files determined on record pickle file.
        if not self.runnable():
            logger.info("Not runnable for this iteration, quitting")
        pass

    def runnable(self):
        # TODO: extract the record.tab file for each repository.
        # Load local block records.
        local_blocks = self.repos["local"].get_locals()
        upstream_blocks = self.repos["local"].get_upstream()

        for key, val in self.repos["upstream"].items():
            record = val["repository"].get_locals()
            blocksnum = val["blocks"]
            # Corresponding blocks record in the local data repository.
            local_record = upstream_blocks[key]
            
            # Check for overlapping blocks taken.
            # For now, blocks are only supported to be taken in a serial fashion. No breaks allowed.
            # Hence we're allowed to check for integrity in easier way:
            if not len(set(record) & set(local_record)) == len(local_record):
                # Hopefully will never encounter below, for that would be structural.
                logger.error("There's an inconsistency in blocks registered")
            
            



