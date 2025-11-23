import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Operations Department"
pattern = r"line_item_data_products*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.unduplicateinator, "order_id"),
            (transform_utils.stringinator, "order_id"),
            (transform_utils.stringinator, "product_name"),
            (transform_utils.stringinator, "product_id")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern)

product_ingester.ingest()