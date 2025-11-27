import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

EXPECTED_SCHEMA = {
    'staff_id':'object',
    'name':'object',
    'job_level':'object',
    'street':'object',
    'state':'object',
    'city':'object',
    'country':'object',
    'contact_number':'object',
    'creation_date':'object',
}

FINAL_SCHEMA = {
    "staff_id": 'string',
    "staff_name": 'string',
    "staff_job_level": 'string',
    "staff_street": 'string',
    "staff_state": 'string',
    "staff_city": 'string',
    "staff_country": 'string',
    "staff_contact_number": 'string',
    "staff_creation_date": 'datetime64[ns]',
}

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Enterprise Department"
pattern = r"staff_data*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'name': 'staff_name',
            'job_level': 'staff_job_level',
            'street': 'staff_street',
            'state': 'staff_state',
            'city': 'staff_city',
            'country': 'staff_country',
            'contact_number': 'staff_contact_number',
            'creation_date': 'staff_creation_date',}),
            (transform_utils.unduplicateinator, "staff_id"),
            (transform_utils.stringinator, "staff_id"),
            (transform_utils.stringinator, "staff_name"),
            (transform_utils.stringinator, "staff_job_level"),
            (transform_utils.stringinator, "staff_street"),
            (transform_utils.stringinator, "staff_state"),
            (transform_utils.stringinator, "staff_city"),
            (transform_utils.stringinator, "staff_country"),
            (transform_utils.numberextractinator, "staff_contact_number"),
            (transform_utils.stringinator, "staff_contact_number"),
            (transform_utils.datetimeinator, "staff_creation_date")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)

product_ingester.ingest()
