import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_SCHEMA = {
    'merchant_id':'object',
    'creation_date':'object',
    'name':'object',
    'street':'object',
    'state':'object',
    'city':'object',
    'country':'object',
    'contact_number':'object',
}

FINAL_SCHEMA = {
    "merchant_id": 'string',
    "merchant_creation_date": 'datetime64[ns]',
    "merchant_name": 'string',
    "merchant_street": 'string',
    "merchant_state": 'string',
    "merchant_city": 'string',
    "merchant_country": 'string',
    "merchant_number": 'string',
}

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
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()