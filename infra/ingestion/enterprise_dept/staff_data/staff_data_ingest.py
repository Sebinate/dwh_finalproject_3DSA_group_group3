import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'name': 'staff_name',
        'job_level': 'staff_job_level',
        'street': 'staff_street',
        'state': 'staff_state',
        'city': 'staff_city',
        'country': 'staff_country',
        'contact_number': 'staff_contact_number',
        'creation_date': 'staff_creation_date',
    }

    df = df.rename(columns=renames)

    return df


# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Enterprise Department"
pattern = r"staff_data*"

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
                batch = transform_utils.unduplicateinator(batch, "staff_id")  
                batch = transform_utils.stringinator(batch, "staff_id")
                batch = transform_utils.stringinator(batch, "staff_name")
                batch = transform_utils.stringinator(batch, "staff_job_level")
                batch = transform_utils.stringinator(batch, "staff_street")
                batch = transform_utils.stringinator(batch, "staff_state")
                batch = transform_utils.stringinator(batch, "staff_city")
                batch = transform_utils.stringinator(batch, "staff_country")
                batch = transform_utils.numberextractinator(batch, "staff_contact_number")
                batch = transform_utils.stringinator(batch, "staff_contact_number")
                batch = transform_utils.datetimeinator(batch, "staff_creation_date") 
                batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

        else:
            data = reader(file_path)
            data = column_renaminator(data)
            data = transform_utils.columndropinator(data)
            data = transform_utils.unduplicateinator(data, "staff_id")  
            data = transform_utils.stringinator(data, "staff_id")
            data = transform_utils.stringinator(data, "staff_name")
            data = transform_utils.stringinator(data, "staff_job_level")
            data = transform_utils.stringinator(data, "staff_street")
            data = transform_utils.stringinator(data, "staff_state")
            data = transform_utils.stringinator(data, "staff_city")
            data = transform_utils.stringinator(data, "staff_country")
            data = transform_utils.numberextractinator(data, "staff_contact_number")
            data = transform_utils.stringinator(data, "staff_contact_number")
            data = transform_utils.datetimeinator(data, "staff_creation_date") 
            data.to_sql(name = staging_table_name, con = engine, if_exists = "append")