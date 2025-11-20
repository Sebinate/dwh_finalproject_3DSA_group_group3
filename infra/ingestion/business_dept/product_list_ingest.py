import os
import glob
from scripts.utils import transform_utils
from scripts.utils import ingest_utils
from scripts.utils import utils

# Make this dynamic in the future
PATH = r"data/Project Dataset-20241024T131910Z-001/Business Department"
pattern = r"product_list*"

file_match_path = os.path.join(PATH, pattern)

#Connecting to db
engine = utils.connect()

file_paths = glob.glob(file_match_path, recursive = True)

cleaners = [(transform_utils.columndropinator,),
            (transform_utils.column_renaminator,),
            (transform_utils.nullinator,),
            (transform_utils.unduplicateinator, "product_id"),
            (transform_utils.stringinator, "product_id"),
            (transform_utils.stringinator, "product_name"),
            (transform_utils.stringinator, "product_type"),
            (transform_utils.floatinator, "product_price"),
            ]

product_ingester = ingest_utils.Ingest(engine = engine, 
                               cleaners = cleaners, 
                               file_paths = file_paths, 
                               pattern = pattern)

product_ingester.ingest()

# if not file_list:
#     print("No new files found")

# else:
#     staging_table_name = pattern.split("*")[0]

#     inspector = inspect(engine)

#     for file_path in file_list:
#         file_type = file_path.split(r"\\")[-1].split(".")[-1]

#         reader = ingest_utils.file_type_reader(file_type)

#         if file_type == "csv" or file_type == "parquet":
#             for batch in reader(file_path):
#                 batch = transform_utils.columndropinator(batch)
#                 batch = column_renaminator(batch)
#                 batch = nullinator(batch)
#                 batch = transform_utils.unduplicateinator(batch, "product_id")  
#                 batch = transform_utils.stringinator(batch, "product_id")
#                 batch = transform_utils.stringinator(batch, "product_name")
#                 batch = transform_utils.stringinator(batch, "product_type")
#                 batch = transform_utils.floatinator(batch, "product_price")
#                 batch = transform_utils.unduplicateinator(batch, "product_id")  
#                 batch.to_sql(name = staging_table_name, con = engine, if_exists = "append")

#         else:
#             data = reader(file_path)
#             data = transform_utils.columndropinator(data)
#             data = column_renaminator(data)
#             data = nullinator(data)
#             data = transform_utils.unduplicateinator(data, "product_id")  
#             data = transform_utils.stringinator(data, "product_id")
#             data = transform_utils.stringinator(data, "product_name")
#             data = transform_utils.stringinator(data, "product_type")
#             data = transform_utils.floatinator(data, "product_price")
#             data = transform_utils.unduplicateinator(data, "product_id")
#             data.to_sql(name = staging_table_name, con = engine, if_exists = "append")