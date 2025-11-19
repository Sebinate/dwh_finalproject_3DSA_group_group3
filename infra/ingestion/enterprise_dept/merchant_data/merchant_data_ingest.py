import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'name': 'merchant_name',
        'creation_date': 'merchant_creation_date',
        'name': 'merchant_name',
        'street': 'merchant_street',
        'state': 'merchant_state',
        'city': 'merchant_city',
        'country': 'merchant_country',
        'contact_number': 'merchant_number',
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Enterprise Department"
pattern = r"merchant_data*"

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
                batch = transform_utils.unduplicateinator(batch, "product_id")  
                batch = transform_utils.stringinator(batch, "merchant_id")
                batch = transform_utils.datetimeinator(batch, "merchant_creation_date")
                batch = transform_utils.stringinator(batch, "merchant_name")
                batch = transform_utils.stringinator(batch, "merchant_street")
                batch = transform_utils.stringinator(batch, "merchant_state")
                batch = transform_utils.stringinator(batch, "merchant_city")
                batch = transform_utils.stringinator(batch, "merchant_country")
                batch = transform_utils.numberextractinator(batch, "merchant_number")
                batch = transform_utils.stringinator(batch, "merchant_number")
                
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            batch = column_renaminator(data)
            data = transform_utils.columndropinator(data)
            data = transform_utils.unduplicateinator(data, "product_id")
            data = transform_utils.stringinator(data, "merchant_id")
            data = transform_utils.datetimeinator(data, "merchant_creation_date")
            data = transform_utils.stringinator(data, "merchant_name")
            data = transform_utils.stringinator(data, "merchant_street")
            data = transform_utils.stringinator(data, "merchant_state")
            data = transform_utils.stringinator(data, "merchant_city")
            data = transform_utils.stringinator(data, "merchant_country")
            data = transform_utils.numberextractinator(data, "merchant_number")
            data = transform_utils.stringinator(data, "merchant_number")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")