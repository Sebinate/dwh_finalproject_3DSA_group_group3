import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Operations Department"
pattern = r"line_item_data_products*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_list = glob.glob(file_match_path, recursive = True)

if not file_list:
    print("No new files found")

else:
    staging_table_name = pattern.split("*")[0]

    inspector = inspect(engine)

    for file_path in file_list:
        file_type = file_path.split(r"\\")[-1].split(".")[-1]

        reader = ingest_utils.file_type_reader(file_type)

        if file_type == "csv" or file_type == "parquet":
            for batch in reader(file_path):
                batch = transform_utils.columndropinator(batch)
                batch = transform_utils.unduplicateinator(batch, "order_id")  
                batch = transform_utils.stringinator(batch, "order_id")
                batch = transform_utils.stringinator(batch, "product_name")
                batch = transform_utils.stringinator(batch, "product_id")
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = transform_utils.columndropinator(data)
            data = transform_utils.unduplicateinator(data, "order_id")  
            batch = transform_utils.stringinator(batch, "order_id")
            batch = transform_utils.stringinator(batch, "product_name")
            batch = transform_utils.stringinator(batch, "product_id")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")