import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Customer Management Department"
pattern = r"user_job*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {'name': 'product_price',
            'job_title': 'user_job_title',
            'job_level': 'user_job_level'}),
            (transform_utils.nullinator, {
            "job_level": "Student"
            }),
            (transform_utils.unduplicateinator, "user_id"),
            (transform_utils.stringinator, "user_id"),
            (transform_utils.stringinator, "user_name"),
            (transform_utils.stringinator, "user_job_title"),
            (transform_utils.stringinator, "user_job_level")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern)

product_ingester.ingest()