import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'name': 'user_name',
        'credit_card_number': 'user_ccn',
        'issuing_bank': 'user_issuing_bank',
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"
pattern = r"user_credit_card*"

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
                batch = transform_utils.columndropinator(batch)
                batch = transform_utils.unduplicateinator(batch, "user_id")  
                batch = transform_utils.stringinator(batch, "user_id")
                batch = transform_utils.stringinator(batch, "user_name")
                batch = transform_utils.stringinator(batch, "user_ccn")
                batch = transform_utils.stringinator(batch, "user_issuing_bank")
                batch = transform_utils.floatinator(batch, "product_price")
                batch = transform_utils.unduplicateinator(batch, "product_id")
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = column_renaminator(data)
            data = transform_utils.columndropinator(data)
            data = transform_utils.unduplicateinator(data, "product_id")
            data = transform_utils.stringinator(data, "user_id")
            data = transform_utils.stringinator(data, "user_name")
            data = transform_utils.stringinator(data, "user_ccn")
            data = transform_utils.stringinator(data, "user_issuing_bank")
            data = transform_utils.floatinator(data, "product_price")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")