from airflow.datasets import Dataset
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from datetime import datetime
import os

project_path = os.getenv("PROJECT_HOME", "/tmp")

def create_ingest_dag(source_name: str, schedule: str, department: str):
    default_args = {
        "owner": "airflow"
    }

    ds = Dataset(f"postgres://postgres_default/airflow/staging/{source_name}")

    dag_id = f"{source_name}_ingest"

    dag = DAG(
        dag_id = dag_id,
        default_args = default_args,
        schedule = schedule,
        catchup = False,
        tags = ['ingestion'],
        start_date = datetime(2020, 1, 1)
    )

    with dag:
        op = DockerOperator(
            task_id = f"{dag_id}",
            image = "ShopZada/to_dw-service:latest",
            command = ['python', f'infra/ingestion/{department}/{source_name}_ingest.py'],
            network_mode = "dwh_finalproject_3dsa_group_group3_default",
            docker_url = 'unix://var/run/docker.sock',
            auto_remove = True,
            outlets = [ds],
            force_pull=False,
            environment = {
                "DB_HOST": "warehouse_db",
                "DB_USER": "admin",
                "DB_PASSWORD": "mypassword",
                "DB_NAME": "shopzada",
                "DB_PORT": "5432"
            },
            mounts=[
                Mount(source=f"{project_path}/data", target="/app/data", type="bind"),
            ],
            working_dir="/app"
        )

    return dag