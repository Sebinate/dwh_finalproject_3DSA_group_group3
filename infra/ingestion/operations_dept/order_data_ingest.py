import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_SCHEMA = {
    'order_id':'object',
    'user_id':'object',
    'estimated arrival':'object',
    'transaction_date':'object',
}

FINAL_SCHEMA = {
    'order_id': 'string',
    'user_id': 'string',
    'transact_estimated_arrival_days': 'Int64',
    'transact_date': 'datetime64[ns]',
}

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Operations Department"
pattern = r"order_data*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'estimated arrival': 'transact_estimated_arrival_days',
            'transaction_date': 'transact_date'}),
            (transform_utils.unduplicateinator, "order_id"),
            (transform_utils.stringinator, "order_id"),
            (transform_utils.stringinator, "user_id"),
            (transform_utils.numberextractinator, "transact_estimated_arrival_days"),
            (transform_utils.intinator, "transact_estimated_arrival_days"),
            (transform_utils.datetimeinator, "transact_date")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()