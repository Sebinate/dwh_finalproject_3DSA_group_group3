import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'estimated arrival': 'transact_estimated_arrival_days',
        'transaction_date': 'transact_date',
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Operations Department"
pattern = r"order_data*"

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
                batch = transform_utils.stringinator(batch, "user_id")
                batch = transform_utils.numberextractinator(batch, "transact_estimated_arrival_days")
                batch = transform_utils.intinator(batch, "transact_estimated_arrival_days")
                batch = transform_utils.datetimeinator(batch, "transact_date")
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = transform_utils.columndropinator(data)
            data = transform_utils.unduplicateinator(data, "order_id")  
            data = transform_utils.stringinator(data, "order_id")
            data = transform_utils.stringinator(data, "user_id")
            data = transform_utils.numberextractinator(data, "transact_estimated_arrival_days")
            data = transform_utils.intinator(data, "transact_estimated_arrival_days")
            data = transform_utils.datetimeinator(data, "transact_date")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")