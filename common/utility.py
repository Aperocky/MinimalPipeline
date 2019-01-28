import os, sys
import shutil

# Helper function dealing with file systems and others.

def remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)
        
  

