import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'creation_date': 'user_creation_date',
        'name': 'user_name',
        'street': 'user_street',
        'state': 'user_state',
        'city': 'user_city',
        'country': 'user_country',
        'birthdate': 'user_birthdate',
        'gender': 'user_gender',
        'device_address': 'user_device_address'
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"
pattern = r"user_data*"

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
                    batch = column_renaminator(batch)
                    batch = transform_utils.unduplicateinator(batch, "user_id")
                    batch = transform_utils.stringinator(batch, "user_id")
                    batch = transform_utils.datetimeinator(batch, "user_creation_date")
                    batch = transform_utils.stringinator(batch, "user_name")
                    batch = transform_utils.stringinator(batch, "user_street")
                    batch = transform_utils.stringinator(batch, "user_state")
                    batch = transform_utils.stringinator(batch, "user_city")
                    batch = transform_utils.stringinator(batch, "user_country")
                    batch = transform_utils.datetimeinator(batch, "user_birthdate")
                    batch = transform_utils.stringinator(batch, "user_gender")
                    batch = transform_utils.stringinator(batch, "user_device_address")
                    batch = transform_utils.stringinator(batch, "user_type")
                    batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = transform_utils.columndropinator(data)
            data = column_renaminator(data)
            data = transform_utils.unduplicateinator(data, "user_id")
            data = transform_utils.stringinator(data, "user_id")
            data = transform_utils.datetimeinator(data, "user_creation_date")
            data = transform_utils.stringinator(data, "user_name")
            data = transform_utils.stringinator(data, "user_street")
            data = transform_utils.stringinator(data, "user_state")
            data = transform_utils.stringinator(data, "user_city")
            data = transform_utils.stringinator(data, "user_country")
            data = transform_utils.datetimeinator(data, "user_birthdate")
            data = transform_utils.stringinator(data, "user_gender")
            data = transform_utils.stringinator(data, "user_device_address")
            data = transform_utils.stringinator(data, "user_type")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")