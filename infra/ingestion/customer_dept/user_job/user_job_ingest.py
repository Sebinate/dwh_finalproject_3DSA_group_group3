import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'name': 'product_price',
        'job_title': 'user_job_title',
        'job_level': 'user_job_level',
    }

    df = df.rename(columns=renames)

    return df

def nullinator(df: pd.DataFrame) -> pd.DataFrame:
    fill_rules = {
        "job_level": "Student",
    }

    df = df.fillna(value=fill_rules)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"
pattern = r"user_job*"

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
                batch = column_renaminator(batch)
                batch = nullinator(batch)
                batch = transform_utils.columndropinator(batch)
                batch = transform_utils.nullinator(batch)
                batch = transform_utils.unduplicateinator(batch, "user_id")  
                batch = transform_utils.stringinator(batch, "user_id")
                batch = transform_utils.stringinator(batch, "user_name")
                batch = transform_utils.stringinator(batch, "user_job_title")
                batch = transform_utils.stringinator(batch, "user_job_level")
                batch.drop("Unnamed: 0", axis = 1).to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = column_renaminator(data)
            data = nullinator(data)
            data = transform_utils.columndropinator(batch)
            data = transform_utils.nullinator(batch)
            data = transform_utils.unduplicateinator(batch, "user_id")  
            data = transform_utils.stringinator(data, "user_id")
            data = transform_utils.stringinator(data, "user_name")
            data = transform_utils.stringinator(data, "user_job_title")
            data = transform_utils.stringinator(data, "user_job_level")
            data = transform_utils.unduplicateinator(data, "product_id")  
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")