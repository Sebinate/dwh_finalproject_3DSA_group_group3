import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_SCHEMA = {
    'order_id':'object',
    'price':'float64',
    'quantity':'object',
}

FINAL_SCHEMA = {
    'order_id': 'string',
    'product_price': 'float64',
    'order_quantity': 'Int64',
}

DATE = os.getenv("TARGET_DATE")
PATH = F"data/Project Dataset-{DATE}*/Operations Department"
pattern = r"line_item_data_prices*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'price': 'product_price',
            'quantity': 'order_quantity'}),
            (transform_utils.unduplicateinator, "order_id"),
            (transform_utils.stringinator, "order_id"),
            (transform_utils.floatinator, "product_price"),
            (transform_utils.numberextractinator, "order_quantity"),
            (transform_utils.intinator, "order_quantity")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()