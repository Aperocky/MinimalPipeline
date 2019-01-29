import configparser as cfg
import importlib
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
        module_path = config.get("source_path", "module")
        class_name = config.get("source_path", "class")
        task_module = importlib.import_module(module_path)
        task_class = getattr(task_module, class_name)
        self.task_instance = task_class()

    # Fill the repository (paths) for the task to be executed
    def initiate_repository(self):
        upstream_paths = list(config.items("upstream_path"))
        local_path = config.get("local_path", "repository_path")
        self.repos["upstream"] = []
        self.repos["local"] = None
        for upstream in upstream_paths:
            self.repos["upstream"].append(Repository(upstream))
        self.repos["local"] = Repository(local_path)
        
    def run(self):
        # TODO: running files determined on record pickle file.




            


