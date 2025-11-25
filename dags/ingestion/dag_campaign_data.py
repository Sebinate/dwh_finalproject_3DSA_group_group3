from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

with DAG('debug_paths_dag', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:

    def print_debug_info():
        print("--- DEBUG INFO START ---")
        
        # 1. Check if the variable exists at all
        p_home = os.getenv("PROJECT_HOME")
        print(f"1. PROJECT_HOME is set to: '{p_home}'")
        
        # 2. Check where Airflow thinks it is running
        cwd = os.getcwd()
        print(f"2. Current Working Directory inside container: '{cwd}'")
        
        # 3. List files in the current directory to see if mounts are working
        print(f"3. Files in {cwd}: {os.listdir(cwd)}")
        
        print("--- DEBUG INFO END ---")

    t1 = PythonOperator(
        task_id='print_environment',
        python_callable=print_debug_info
    )