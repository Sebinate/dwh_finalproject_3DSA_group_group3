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