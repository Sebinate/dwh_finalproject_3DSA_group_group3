import pandas as pd
import pickle as pkl
import pyarrow.parquet as pq

from typing import Iterator
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Engine, inspect
from scripts.utils import transform_utils, schema_utils

def csv_reader(path: str, chunksize: int = 10_000) -> Iterator[pd.DataFrame]:
    data = pd.read_csv(path, chunksize = chunksize, sep = None, index_col = False)
    return data

def html_reader(path: str) -> list[pd.DataFrame]:
    """
    Note: pd.read_html works fine, but in cases where there are multiple tables/incorrect
    table formats, this is preferred 
    """
    with open(path, mode = 'r', encoding = "utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")
    
    tables = soup.find_all('table')
    dfs = [] 
    
    for table in tables:
        data = pd.read_html(str(table))[0]
        dfs.append(data)
        
    return dfs[0]

def parquet_reader(path: str, chunksize: int = 10_000) -> Iterator[pd.DataFrame]:
    pf = pq.ParquetFile(path)
    batches = pf.iter_batches(batch_size = chunksize)
    
    for batch in batches:
        yield batch.to_pandas()

def json_reader(path: str) -> pd.DataFrame:
    data = pd.read_json(path, orient = "records")
    return data

def pkl_reader(path: str) -> pd.DataFrame:
    with open(path, 'rb') as file:
        data = pkl.load(file)

    return data

def xlsx_reader(path: str) -> pd.DataFrame:
    data = pd.read_excel(path, engine = "openpyxl")
    return data

def file_type_reader(file_type: str):
    if file_type == "csv":
        return csv_reader
    
    elif file_type == "parquet":
        return parquet_reader
    
    elif file_type == "xlsx":
        return xlsx_reader
    
    elif file_type == "pickle" or file_type == "pkl":
        return pkl_reader
    
    elif file_type == "html":
        return html_reader
    
    elif file_type == "json":
        return json_reader

# ingest_utils.py
class Ingest:
    def __init__(self, 
                 engine: Engine,
                 cleaners: list[tuple],
                 file_paths: list,
                 pattern: str,
                 expected_schema: list[str], 
                 mismatch_folder: str = "/app/data/schema_mismatches"):
        self.engine = engine
        self.cleaners = cleaners
        self.file_paths = file_paths
        self.pattern = pattern
        self.expected_schema = expected_schema 
        self.mismatch_folder = mismatch_folder 
    
    def ingest(self):
            if not self.file_paths:
                print("No new files found")
                return
            staging_table_name = self.pattern.split("*")[0]
            
            for file_path in self.file_paths:
                file_type = file_path.split(r"\\")[-1].split(".")[-1]
                reader = file_type_reader(file_type)
                if file_type == "csv" or file_type == "parquet":
                    for batch in reader(file_path):
                        is_valid, batch = schema_utils.validate_schema(
                            df=batch, 
                            expected_schema_map=self.expected_schema, 
                            mismatch_folder=self.mismatch_folder,
                            file_path=file_path
                        )          
                        if not is_valid:
                            continue
                        for cleaner in self.cleaners:
                            if len(cleaner) > 1:
                                batch = cleaner[0](batch, *cleaner[1:])
                            else:
                                batch = cleaner[0](batch)
                        batch.to_sql(name = staging_table_name, con = self.engine, if_exists = "append", schema = "staging")

                else:
                    data = reader(file_path)
                    is_valid, data = schema_utils.validate_schema(
                        df=data, 
                        expected_schema_map=self.expected_schema, 
                        mismatch_folder=self.mismatch_folder,
                        file_path=file_path
                    )
                    if not is_valid:
                        continue 
                    for cleaner in self.cleaners:
                        if len(cleaner) > 1:
                            data = cleaner[0](data, *cleaner[1:])
                        else:
                            data = cleaner[0](data)
                    data.to_sql(name = staging_table_name, con = self.engine, if_exists = "append", schema = "staging")

if __name__ == "__main__":
    data = pkl_reader(r"C:\Users\User\dwh_finalproject_3DSA_group_group3\data\Project Dataset-20241024T131910Z-001\Customer Management Department\user_credit_card.pickle")
    print(data)