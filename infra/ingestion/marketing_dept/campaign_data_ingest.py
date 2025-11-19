import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'discount': 'campaign_discount',
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Marketing Department"
pattern = r"campaign_data*"

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
                batch = transform_utils.unduplicateinator(batch, "campaign_id")  
                batch = transform_utils.stringinator(batch, "campaign_id")
                batch = transform_utils.stringinator(batch, "campaign_name")
                batch = transform_utils.numberextractinator(batch, "campaign_discount")
                batch = transform_utils.floatinator(batch, "campaign_discount")
                batch = transform_utils.percentinator(batch, "campaign_discount")
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = transform_utils.columndropinator(data)
            data = transform_utils.unduplicateinator(data, "campaign_id")  
            data = transform_utils.stringinator(data, "campaign_id")
            data = transform_utils.stringinator(data, "campaign_name")
            data = transform_utils.numberextractinator(data, "campaign_discount")
            data = transform_utils.floatinator(data, "campaign_discount")
            data = transform_utils.percentinator(data, "campaign_discount")
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")