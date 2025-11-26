import pandas as pd
import os
from datetime import datetime
from typing import Union, Tuple

def validate_schema(
    df: pd.DataFrame, 
    expected_schema_map: dict[str, type],
    mismatch_folder: str,
    file_path: str = None
) -> Tuple[bool, pd.DataFrame]:
    expected_columns = list(expected_schema_map.keys())
    df_columns = df.columns.tolist()
    is_valid = set(df_columns) == set(expected_columns)
    
    if not is_valid:
        print(f"Expected {expected_columns} found {df_columns})")
        
        os.makedirs(mismatch_folder, exist_ok=True)
        filename = os.path.basename(file_path) if file_path else "mismatch_data"
        clean_filename = os.path.splitext(filename)[0].replace(".", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") #para pag may multiple similar named files di mag overwrite
        mismatch_file_name = f"{clean_filename}_{timestamp}_schema_mismatch_name.parquet"    
        mismatch_output_path = os.path.join(mismatch_folder, mismatch_file_name)
        try:
            df.to_parquet(mismatch_output_path, index=False)
            print(f"column name mismatch, saved to {mismatch_output_path}")
        except Exception as e:
            print(e)
        return False, df
    
    print("Schema name OK, checking col types")
    try:
        df = df.astype(expected_schema_map, errors='raise') 
        is_valid = True

    except Exception as e:
        is_valid = False
        print(e)
        os.makedirs(mismatch_folder, exist_ok=True)
        filename = os.path.basename(file_path) if file_path else "mismatch_data"
        clean_filename = os.path.splitext(filename)[0].replace(".", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") #para pag may multiple similar named files di mag overwrite
        mismatch_file_name = f"{clean_filename}_{timestamp}_schema_mismatch_type.parquet"
        mismatch_output_path = os.path.join(mismatch_folder, mismatch_file_name)
        try:
            df.to_parquet(mismatch_output_path, index=False)
            print(f"column type mismatch, saved to {mismatch_output_path}")
        except Exception as e2:
            print(e2)
            
        return False, df
    print("Schema type OK")
    return True, df