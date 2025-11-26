import pandas as pd
import hashlib
import re

def whitespacedestroyer(text:str) -> str:
    text = str(text) #__china china__ space
    text = text.strip() #__china china__
    text = re.sub(r'\s+', '_', text)  #__china_china__
    text = re.sub(r'_+', '_', text) #_china_china_
    text = text.strip('_') #china_china 
    return text

def numberextractinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = df[column].astype(str).str.replace(r'[^0-9]', '', regex=True)
    return df

def floatinator(df: pd.DataFrame, column: str) -> pd.DataFrame: #also removes periods btw
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
        return
        
    return df

def intinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors='coerce').astype(int)
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
        return
    return df

def percentinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = (df[column] / 100).round(2)
    return df

def columndropinator(df: pd.DataFrame) -> pd.DataFrame:
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    return df

def stringinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = df[column].astype(str)
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
        return
    return df

def datetimeinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        try:
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
        except Exception as e:
            print(f"Error converting column '{column}' to datetime: {e}")
            return
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
        return
    return df

def unduplicateinator(df: pd.DataFrame, columns) -> pd.DataFrame:
    df = df.drop_duplicates(subset=columns)
    return df

def boolinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        truthy = {"true", "1", "yes", "y", "t"}
        falsy = {"false", "0", "no", "n", "f"}
        df[column] = (df[column]
            .astype(str)
            .str.strip()
            .str.lower()
            .map(lambda x: True if x in truthy
                           else False if x in falsy
                           else pd.NA)
            .astype("boolean"))
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
        return
    return df

def column_renaminator(df: pd.DataFrame, renames: dict) -> pd.DataFrame:
    df = df.rename(columns=renames)
    print(df.columns)

    return df

def nullinator(df: pd.DataFrame, fill_rules: dict) -> pd.DataFrame:
    df = df.fillna(value=fill_rules)

    return df

def hashinator(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    def _hash_value(value: str) -> str:
        try:
            sha256 = hashlib.sha256() 
            encoded_value = str(value).encode('utf-8')
            sha256.update(encoded_value) 
            return sha256.hexdigest() 
        except Exception:
            return "SOMETHING WRONG!"
    df[column_name] = df[column_name].apply(_hash_value)
    print (f"Hashed {column_name}")
    return df