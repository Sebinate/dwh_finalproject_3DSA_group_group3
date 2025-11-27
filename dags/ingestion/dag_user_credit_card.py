import sys
import os

sys.path.insert(0, "/opt/airflow/dags")

from ingestion.builder import create_ingest_dag

dag = create_ingest_dag(
    source_name="user_credit_card",
    schedule = "@daily",
    department = "customer_dept"
)