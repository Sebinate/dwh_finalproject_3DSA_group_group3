import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_SCHEMA = {
    'order_id':'object',
    'product_name':'object',
    'product_id':'object',
}

FINAL_SCHEMA = {
    'order_id': 'string',
    'product_name': 'string',
    'product_id': 'string',
}


DATE = os.getenv("TARGET_DATE")
PATH = f"data/Project Dataset-{DATE}*/Operations Department"
pattern = r"line_item_data_products*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.stringinator, "order_id"),
            (transform_utils.stringinator, "product_name"),
            (transform_utils.stringinator, "product_id")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()