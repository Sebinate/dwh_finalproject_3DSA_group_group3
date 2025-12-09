import sys
import os

sys.path.insert(0, "/opt/airflow/dags")

from ingestion.builder import create_ingest_dag

dag = create_ingest_dag(
    source_name="merchant_data",
    schedule = "0 0 24 10 *",
    department = "enterprise_dept"
)