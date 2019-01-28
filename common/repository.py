import os, sys
import pickle
from . import utility as ut

# The repository.py loads a physical location for task to process.

# Among loaded will be a unique ID for jobs so it could decide if it should run or not.

class Repository:

    def __init__(self, location):
        self.location = location
        self.indexes = os.path.join(self.location, "record.tab")
        # The two lock files are temporary files that will exist if the Repository is under lock.
        self.read_lock = os.path.join(self.location, "read_lock")
        self.write_lock = os.path.join(self.location, "write_lock")
        self.load_index()

    # Create repository when it doesn't exist (first run)
    def create_repository(self):
        try:
            os.mkdir(self.location)
        except:
            raise FileExistsError("This repository already exist")
        record = {"upstream": [], "local": []}
        pickle.dump(record, open(self.indexes, 'rb'))

        

    def load_index(self):
        self.record = pickle.load(open(self.indexes, "rb"))
        self.upstream = self.record["upstream"]
        self.locals = self.record["local"]

    # locals indicate records within current task pipelines
    def get_locals(self):
        return self.locals

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

