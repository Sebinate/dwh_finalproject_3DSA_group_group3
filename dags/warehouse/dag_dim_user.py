from airflow.datasets import Dataset
from warehouse.builder import create_to_dw_dag

pl1 = Dataset("postgres://postgres_default/airflow/staging/user_credit_card")
pl2 = Dataset("postgres://postgres_default/airflow/staging/user_job")
pl3 = Dataset("postgres://postgres_default/airflow/staging/user_data")

waiting_for = [pl1, pl2, pl3]

sql_file_name = r"/app/sql/dim_user.sql"

dag = create_to_dw_dag(
    source_name = "dim_user",
    schedule = waiting_for,
    sql_file_name = sql_file_name
)