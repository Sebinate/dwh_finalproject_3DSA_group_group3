import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'delay in days': 'order_delay_days',
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Operations Department"
pattern = r"order_delays*"

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
                batch = transform_utils.unduplicateinator(batch, "order_id")  
                batch = transform_utils.stringinator(batch, "order_id")
                batch = transform_utils.numberextractinator(batch, "order_delay_days")
                batch = transform_utils.intinator(batch, "order_delay_days")
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = transform_utils.columndropinator(data)
            data = column_renaminator(data)
            data = transform_utils.unduplicateinator(data, "order_id")  
            data = transform_utils.stringinator(data, "order_id")
            data = transform_utils.numberextractinator(data, "order_delay_days")
            data = transform_utils.intinator(data, "order_delay_days")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")