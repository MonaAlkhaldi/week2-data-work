from pathlib import Path
from bootcamp_data.config import make_paths 

#Trying if paths in config.py works ---> It dose :)
paths = make_paths(Path("."))  # current project root
print("path is " , paths)


#--------------------------------------------
#Checking if the io.py functions works 

from bootcamp_data.io import (
    read_orders_csv,
    read_users_csv,
    write_parquet,
    read_parquet,
)

print("io.py imports work ✅")
