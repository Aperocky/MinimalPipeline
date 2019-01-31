import os, sys
import pickle
import time
from . import utility as ut
from . import logs
from . import envs

logger = logs.get_logger(__name__)

"""
The repository.py loads a physical location for task to process.
Each task will be assigned a repository.
The repository contains
0. blocks that contains data
1. record object that keeps track of the blocks.
2. read and write locks that prevents corruption of files.

The repository handles block on the top level, it does not control
anything below block level. what's inside the block is considered 
invisible and irrelevant to the Repository class.
"""

class Repository:

    def __init__(self, name):
        location = envs.get_env_var("paths", "repository_path") 
        self.location = os.path.join(os.environ["pipeline_path"], location, name)
        self.indexes = os.path.join(self.location, "record.tab")
        # The two lock files are temporary files that will exist if the Repository is under lock.
        self.read_lock = os.path.join(self.location, "read_lock")
        self.write_lock = os.path.join(self.location, "write_lock")
        self.blocks = os.path.join(self.location, "blocks")
        self.load_index()

    # Create repository when it doesn't exist (first run)
    def create_repository(self):
        logger.info("Creating repository {} as it previously didn't exist".format(self.location))
        ut.create_dir(self.location)
        ut.create_dir(os.path.join(self.location, "blocks"))
        record = {"upstream": [], "local": []}
        pickle.dump(record, open(self.indexes, 'wb'))

    def load_index(self):
        if not os.path.isdir(self.location):
            self.create_repository()
        self.record = pickle.load(open(self.indexes, "rb"))
        self.upstream = self.record["upstream"]
        self.local = self.record["local"]

    # locals indicate records within current task pipelines
    def get_locals(self):
        return self.local

    # upstream indicate records on previous task pipelines.
    def get_upstream(self):
        return self.upstream

    def write_locked(self):
        return os.path.isfile(self.write_lock)

    def read_locked(self):
        return os.path.isfile(self.read_lock)

    def lock_read(self):
        if not self.read_locked():
            open(self.read_lock, "w").close()

    def lock_write(self):
        if not self.write_locked():
            open(self.write_lock, "w").close()

    def unlock_read(self):
        os.remove(self.read_lock)

    def unlock_write(self):
        os.remove(self.write_lock)

    def get_block(self, blockid):
        # blockid: 6 digit string of digit padded with 0.
        # there should never be 1M block in a single repository.
        block_path = os.path.join(self.blocks, blockid)
        # Check if block actually exists.
        if not os.path.isdir(block_path):
            logger.error("Block path {} does not exist".format(block_path))
            raise FileNotFoundError("Block path does not exist")
        return block_path

    def create_block(self):
        block_id = 0 # 0 is the natural start of a number sequence.
        # lazy search O(n), TODO implement binary search.
        numstr = lambda x: str(x).zfill(6)
        get_block = lambda x: os.path.join(self.blocks, x)
        while True:
            block_path = get_block(numstr(block_id))
            if not os.path.isdir(block_path):
                ut.create_dir(block_path)
                break
            block_id += 1
        # This function assume that block is processed after giving the path.
        logger.info("Created block {}".format(block_path))
        self.local.append((numstr(block_id), int(time.time())) 
        return block_path
