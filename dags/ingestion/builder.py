from airflow.datasets import Dataset
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from datetime import datetime, timedelta
from dotenv import load_dotenv

import os

load_dotenv("/opt/airflow/.env")

project_path = os.getenv("PROJECT_HOME")
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def create_ingest_dag(source_name: str, schedule: str, department: str):
    default_args = {
        "owner": "airflow",
        # "retries": 10,
        # "retry_delay": timedelta(minutes=10),
    }

    ds = Dataset(f"postgres://postgres_default/airflow/staging/{source_name}")

    dag_id = f"{source_name}_ingest"

    dag = DAG(
        dag_id = dag_id,
        default_args = default_args,
        schedule = schedule,
        catchup = True,
        tags = ['ingestion'],
        start_date = datetime(2023, 10, 24),
        max_active_runs=1
    )

    with dag:
        op = DockerOperator(
            task_id = f"{dag_id}",
            image = "ShopZada/ingestor-service:latest",
            command = ['python', f'infra/ingestion/{department}/{source_name}_ingest.py'],
            network_mode = "dwh_finalproject_3dsa_group_group3_default",
            docker_url = 'unix://var/run/docker.sock',
            auto_remove = False,
            outlets = [ds],
            force_pull=False,
            environment = {
                "DB_HOST": "warehouse_db",
                "DB_USER": DB_USER,
                "DB_PASSWORD": DB_PASSWORD,
                "DB_NAME": "shopzada",
                "DB_PORT": "5432",
                "TARGET_DATE": "{{ data_interval_end | ds_nodash }}"
            },
            mounts=[
                Mount(source=fr"{project_path}/data", target="/app/data", type="bind"),
            ],
            working_dir="/app"
        )

    return dag