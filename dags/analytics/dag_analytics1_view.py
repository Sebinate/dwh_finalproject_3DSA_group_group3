from airflow.datasets import Dataset
from analytics.builder import create_analytics_dag

pl1 = Dataset("postgres://postgres_default/airflow/dw/fact_transaction")

waiting_for = [pl1, ]

sql_file_name = r"/app/sql/view1.sql"

dag = create_analytics_dag(
    source_name = "analytic_view1",
    schedule = waiting_for,
    sql_file_name = sql_file_name
)