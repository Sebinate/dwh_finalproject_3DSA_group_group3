import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Enterprise Department"
pattern = r"merchant_data*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'name': 'merchant_name',
            'creation_date': 'merchant_creation_date',
            'name': 'merchant_name',
            'street': 'merchant_street',
            'state': 'merchant_state',
            'city': 'merchant_city',
            'country': 'merchant_country',
            'contact_number': 'merchant_number'}),
            (transform_utils.unduplicateinator, "product_id"),
            (transform_utils.stringinator, "merchant_id"),
            (transform_utils.datetimeinator, "merchant_creation_date"),
            (transform_utils.stringinator, "merchant_name"),
            (transform_utils.stringinator, "merchant_street"),
            (transform_utils.stringinator, "merchant_state"),
            (transform_utils.stringinator, "merchant_city"),
            (transform_utils.stringinator, "merchant_country"),
            (transform_utils.numberextractinator, "merchant_number"),
            (transform_utils.stringinator, "merchant_country")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern)

product_ingester.ingest()