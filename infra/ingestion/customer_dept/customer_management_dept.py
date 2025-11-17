import os
import pandas as pd
from scripts.utils import ingest_utils
from scripts.utils import utils

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"

#Connecting to db
engine = utils.connect()

for file in os.listdir(PATH):
    file_name = file.split(".")[0]
    file_type = file.split(".")[-1]

    file_path = os.path.join(PATH, file)

    reader = ingest_utils.file_type_reader(file_type)    

    if file_type == "csv" or file_type == "parquet":
        next(reader(file_path)).head(0).to_sql(name = file_name, con = engine, if_exists = "replace") #Remove once in production
        for batch in reader(file_path):
            batch.to_sql(name = file_name, con = engine, if_exists = "append")

    else:
        data = reader(file_path)
        data.head(0).to_sql(name = file_name, con = engine, if_exists = "replace") #Remove once in production
        data.to_sql(name = file_name, con = engine, if_exists = "append")