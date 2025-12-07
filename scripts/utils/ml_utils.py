import os
import joblib
import numpy as np

def to_pkl(artifact_dir, model_name, model):
    """For reproducibility purposes, the preprocessor and model pickle file will be saved"""
    file_path = os.path.join(artifact_dir, f"{model_name}.pkl")
    joblib.dump(model, file_path)

def to_np_arr(name:str, **array):
    """For reproducibility purposes, the train and test data split numpy 
    arrays will be saved"""
    
    np.savez(name, **array)

def from_pkl(file_path: str):
    model = joblib.load(file_path)
    return model