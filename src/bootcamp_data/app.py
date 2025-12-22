from pathlib import Path
from config import make_paths 

#Trying if paths in config.py works ---> It dose :)
paths = make_paths(Path("."))  # current project root
print(paths)