from airflow.datasets import Dataset
from warehouse.builder import create_to_dw_dag

pl = Dataset("postgres://postgres_default/airflow/staging/product_list")

waiting_for = [pl, ]

sql_file_name = r"/app/sql/dim_product.sql"

dag = create_to_dw_dag(
    source_name = "dim_product",
    schedule = waiting_for,
    sql_file_name = sql_file_name
)