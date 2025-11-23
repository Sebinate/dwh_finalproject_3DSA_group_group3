import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"
pattern = r"user_data*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'creation_date': 'user_creation_date',
            'name': 'user_name',
            'street': 'user_street',
            'state': 'user_state',
            'city': 'user_city',
            'country': 'user_country',
            'birthdate': 'user_birthdate',
            'gender': 'user_gender',
            'device_address': 'user_device_address'}),
            (transform_utils.unduplicateinator, "user_id"),
            (transform_utils.stringinator, "user_id"),
            (transform_utils.datetimeinator, "user_creation_date"),
            (transform_utils.stringinator, "user_name"),
            (transform_utils.stringinator, "user_street"),
            (transform_utils.stringinator, "user_state"),
            (transform_utils.stringinator, "user_city"),
            (transform_utils.stringinator, "user_country"),
            (transform_utils.datetimeinator, "user_birthdate"),
            (transform_utils.stringinator, "user_gender"),
            (transform_utils.stringinator, "user_device_address"),
            (transform_utils.stringinator, "user_type")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern)

product_ingester.ingest()


