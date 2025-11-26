import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_PRODUCT_SCHEMA = {
    'Unnamed: 0':'int64',
    'transaction_date':'object',
    'campaign_id':'object',
    'order_id':'object',
    'estimated arrival':'object',
    'availed':'int64',
}

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Marketing Department"
pattern = r"transactional_campaign_data*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'transaction_date': 'transact_date',
            'estimated arrival': 'transact_estimated_arrival_days',
            'availed': 'transact_availed',}),
            (transform_utils.unduplicateinator, "campaign_id"),
            (transform_utils.datetimeinator, "transact_date"),
            (transform_utils.stringinator, "campaign_id"),
            (transform_utils.stringinator, "order_id"),
            (transform_utils.numberextractinator, "transact_estimated_arrival_days"),
            (transform_utils.intinator, "transact_estimated_arrival_days"),
            (transform_utils.boolinator, "transact_availed")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_PRODUCT_SCHEMA)

product_ingester.ingest()