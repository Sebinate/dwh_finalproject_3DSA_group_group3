from airflow.datasets import Dataset
from warehouse.builder import create_to_dw_dag

pl = Dataset("postgres://postgres_default/airflow/staging/order_data")

waiting_for = [pl, ]

sql_file_name = r"/app/sql/dim_order.sql"

dag = create_to_dw_dag(
    source_name = "dim_order",
    schedule = waiting_for,
    sql_file_name = sql_file_name
)