from airflow.datasets import Dataset
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
project_path = os.getenv("PROJECT_HOME")

def create_analytics_dag(source_name: str, schedule: list, sql_file_name: str):
    default_args = {
        "owner": "airflow"
    }

    dag_id = f"{source_name}_analytics"

    dag = DAG(
        dag_id = dag_id,
        default_args = default_args,
        schedule = schedule,
        catchup = False,
        tags = ['analytics'],
        start_date=datetime(2020, 1, 1)
    )

    with dag:
        op = DockerOperator(
            task_id = f"{dag_id}",
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
                "SQL_FILE": sql_file_name
            })
    return dag