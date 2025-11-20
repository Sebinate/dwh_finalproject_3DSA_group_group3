import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"
pattern = r"user_credit_card*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'name': 'user_name',
            'credit_card_number': 'user_ccn',
            'issuing_bank': 'user_issuing_bank',}),
            (transform_utils.unduplicateinator, "user_id"),
            (transform_utils.stringinator, "user_id"),
            (transform_utils.stringinator, "user_name"),
            (transform_utils.stringinator, "user_ccn"),
            (transform_utils.stringinator, "user_issuing_bank"),
            (transform_utils.floatinator, "product_price")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern)

product_ingester.ingest()
