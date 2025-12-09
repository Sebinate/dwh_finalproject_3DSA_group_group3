import os
import pandas as pd
import glob
from scripts.utils import ingest_utils
from scripts.utils import utils
from scripts.utils import transform_utils

EXPECTED_SCHEMA = {
    'order_id':'object',
    'merchant_id':'object',
    'staff_id':'object',
}

FINAL_SCHEMA = {
    "order_id": 'string',
    "merchant_id": 'string',
    "staff_id": 'string',
}

DATE = os.getenv("TARGET_DATE")
PATH = f"data/Project Dataset-{DATE}*/Enterprise Department"
pattern = r"order_with_merchant*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.unduplicateinator, "order_id"),
            (transform_utils.stringinator, "order_id"),
            (transform_utils.stringinator, "merchant_id"),
            (transform_utils.stringinator, "staff_id")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()