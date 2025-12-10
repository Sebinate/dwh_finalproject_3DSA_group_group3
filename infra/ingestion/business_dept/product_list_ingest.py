import os
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import schema_utils
from scripts.utils import utils
#pre transform
EXPECTED_SCHEMA = {
    'product_id': 'object',
    'product_name': 'object',
    'product_type': 'object',
    'price': 'float64',
}

#post transform
FINAL_SCHEMA = {
    "product_id": 'string',
    "product_name": 'string',
    "product_type": 'string',
    "product_price": 'float64',
}

DATE = os.getenv("TARGET_DATE")
PATH = f"data/Project Dataset-{DATE}*/Business Department"
pattern = r"product_list*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,
            {"price": "product_price",
            "Name": "product_name"}),
            (transform_utils.nullinator, {
            "product_type": "Unknown"
            }),
            (transform_utils.unduplicateinator, "product_id"),
            (transform_utils.stringinator, "product_id"),
            (transform_utils.stringinator, "product_name"),
            (transform_utils.stringinator, "product_type"),
            (transform_utils.floatinator, "product_price")
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern,
                               expected_schema = EXPECTED_SCHEMA,
                               final_schema = FINAL_SCHEMA)
product_ingester.ingest()