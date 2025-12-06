from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

import os
from datetime import datetime
from dotenv import load_dotenv


pl1 = Dataset("postgres://postgres_default/airflow/dw/fact_transaction")

load_dotenv("/opt/airflow/.env")
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
project_path = os.getenv("PROJECT_HOME")

default_args = {
        "owner": "airflow"
    }

with DAG(
        dag_id = "machine_learning_pipeline",
        default_args = default_args,
        schedule = [pl1, ],
        catchup = False,
        tags = ['analytics'],
        start_date=datetime(2020, 1, 1)
    ):
    op1 = DockerOperator(
        task_id = f"materializing_view",
        image = "ShopZada/to_dw-service:latest",
        command = ['python', f'infra/transform_to_dw/sql_reader.py'],
        network_mode = "dwh_finalproject_3dsa_group_group3_default",
        docker_url = 'unix://var/run/docker.sock',
        auto_remove = False,
        force_pull=False,
        environment = {
            "DB_HOST": "warehouse_db",
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
            "DB_NAME": "shopzada",
            "DB_PORT": "5432",
            "SQL_FILE": r"/app/sql/ML_view.sql"
        })
    
    op2 = ""

    op3 = ""

    op4 = ""