import configparser as cfg
import importlib
import sys
from common import utility as ut
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
            local_repository = Repository(local_path)
            self.repos["local"] = local_repository

            # Add upstream when there isn't for local_repo.
            # This is important as repository can be initiated for first time and not know its upstreams.
            # And also work if the upstream exists.
            for key in self.repos["upstream"].keys():
                local_repoository.add_upstream(key, [])
            
        except BlockingIOError as e:
            logger.warning("Not all repository are readable, awaiting next execution")
            sys.exit()

        
    def run(self):
        # TODO: running files determined on record pickle file.
        if not self.runnable():
            logger.info("Not runnable for this iteration, quitting")
            return
        self.construct_temp()
        task_config = self.config.get("source_path", "config_path")
        try:
            status = self.task_instance.run(self.input_paths, self.output_path, task_config)
        except:
            status = False
            logger.error("Error was generated while running the task")

        # Manage post task issues.
        if status:
            # Update record.tab objects for local
            local_repo = self.repos["local"]
            for key, tabs in self.upstream_blocks.items():
                local_repo.add_upstream(key, tabs)
            # Copy temp folder into correct place.
            curr_block = local_repo.create_block()
            logger.info("Moving created folder into blocks")
            ut.copy_directory(self.output_path, curr_block, overwrite=True)
            # Save repository record and unlock it.
            local_repo.save_index()
            local_repo.unlock_read()
        else:
            logger.warning("Execution deemed flawed, terminating")
            sys.exit()

    def runnable(self):
        # Load local block records.
        local_blocks = self.repos["local"].get_locals()
        upstream_records = self.repos["local"].get_upstream()

        upstream_blocks = {}
        execute = True

        for key, val in self.repos["upstream"].items():
            upstream_blocks[key] = []
            record = val["repository"].get_locals()
            blocksnum = val["blocks"]
            # Corresponding blocks record in the local data repository.
            local_record = upstream_records[key]
            
            # Check for overlapping blocks taken.
            # For now, blocks are only supported to be taken in a serial fashion. No breaks allowed.
            # Hence we're allowed to check for integrity in easier way:
            if not len(set(record) & set(local_record)) == len(local_record):
                # Hopefully will never encounter below, for that would be structural.
                logger.error("There's an inconsistency in blocks registered")
                raise ValueError("Blocks found to be inconsistent")

            new_blocks = len(record) - len(local_record)
            start_num = len(local_record)
            if new_blocks >= blocknum:
                upstream_blocks[key].extend(record[start_num: startnum+blocksnum])
            else:
                logger.warning("Repository {} does not contain enough new blocks to proceed".format(key))
                execute = False
                break
             
        if execute:
            self.upstream_blocks = upstream_blocks
            return True
        return False

    # Create temporary folder containing the upstream blocks
    def construct_temp(self):
        self.input_paths = {}
        self.output_path = None
        # Create input paths.
        for key, blocks in self.upstream_blocks.items():
            self.input_paths[key] = ut.create_temp_dir()
            repository = self.repos["upstream"][key]["repository"]
            for each in blocks:
                blockid = each[0] # (blockid, timestamp)
                block_path = repository.get_block(blockid)
                to_path = os.path.join(self.input_paths[key], blockid)
                logger.info("Creating block {} from {} in temp directory".format(blockid, key))
                ut.copy_directory(block_path, to_path)
        self.output_path = ut.create_temp_dir()
        # Unlock upstream repositories as they're no longer used.
        for each in self.repos["upstream"]:
            each.unlock_read()


