import pandas as pd
from typing import Iterator
import pickle as pkl
from bs4 import BeautifulSoup
import pyarrow.parquet as pq

def csv_reader(path: str, chunksize: int = 10_000) -> Iterator[pd.DataFrame]:
    data = pd.read_csv(path, chunksize = chunksize, sep = None)
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
        
    return dfs

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

if __name__ == "__main__":
    pass