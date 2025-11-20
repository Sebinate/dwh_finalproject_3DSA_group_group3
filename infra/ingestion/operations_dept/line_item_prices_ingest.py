import os
import pandas as pd
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils
from sqlalchemy import inspect

def column_renaminator(df: pd.DataFrame) -> pd.DataFrame:
    renames = {
        'price': 'product_price',
        'quantity': 'order_quantity',
    }

    df = df.rename(columns=renames)

    return df

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Operations Department"
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
                               pattern = pattern)

product_ingester.ingest()