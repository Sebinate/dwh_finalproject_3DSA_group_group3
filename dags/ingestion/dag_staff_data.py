import sys
import os

sys.path.insert(0, "/opt/airflow/dags")

from ingestion.builder import create_ingest_dag

dag = create_ingest_dag(
    source_name="staff_data",
    schedule = "@daily",
    department = "enterprise_dept"
)