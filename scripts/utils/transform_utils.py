import pandas as pd
import re

def whitespacedestroyer(text:str) -> str:
    text = str(text) #__china china__ space
    text = text.strip() #__china china__
    text = re.sub(r'\s+', '_', text)  #__china_china__
    text = re.sub(r'_+', '_', text) #_china_china_
    text = text.strip('_') #china_china 
    return text

def numberextractor(text:str) -> float:
    finder = re.search(r'\d+(\.\d+)?', str(text))
    if finder:
        return float(finder.group(0))
    else:
        #ibang function ba yung pag convert ng four to 4 etc?
        return 0

def lowercaseinator(text:str) -> str:
    text = str(text)
    text = text.lower
    return text

def columndropinator(df: pd.DataFrame) -> pd.DataFrame:
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    else:
        print("'Unnamed: 0' is not present here")
    return df

def stringinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = df[column].astype(str)
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
    return df

def floatinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = df[column].astype(float)
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
    return df

def datetimeinator(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        try:
            df[column] = pd.to_datetime(df[column], errors='coerce')
        except Exception as e:
            print(f"Error converting column '{column}' to datetime: {e}")
    else:
        print(f"Column '{column}' does not exist in the DataFrame.")
    return df