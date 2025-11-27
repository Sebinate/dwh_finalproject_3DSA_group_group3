import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_SCHEMA = {
    'Unnamed: 0':'int64',
    'campaign_id':'object',
    'campaign_name':'object',
    'campaign_description':'object',
    'discount':'object',
}

FINAL_SCHEMA = {
    "campaign_id": 'string',
    "campaign_name": 'string',
    "campaign_description": 'string',
    "campaign_discount": 'float64',
}

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Marketing Department"
pattern = r"campaign_data*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'discount': 'campaign_discount'}),
            (transform_utils.unduplicateinator, "campaign_id"),
            (transform_utils.stringinator, "campaign_id"),
            (transform_utils.stringinator, "campaign_name"),
            (transform_utils.numberextractinator, "campaign_discount"),
            (transform_utils.floatinator, "campaign_discount"),
            (transform_utils.percentinator, "campaign_discount")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()