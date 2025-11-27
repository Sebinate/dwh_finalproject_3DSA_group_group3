from airflow.datasets import Dataset
from warehouse.builder import create_to_dw_dag

req1 = Dataset("postgres://postgres_default/airflow/staging/line_item_prices")
req2 = Dataset("postgres://postgres_default/airflow/staging/order_delays")
req3 = Dataset("postgres://postgres_default/airflow/staging/line_item_products")
req4 = Dataset("postgres://postgres_default/airflow/staging/order_data")
req5 = Dataset("postgres://postgres_default/airflow/dw/dim_user")
req6 = Dataset("postgres://postgres_default/airflow/dw/dim_order")
req7 = Dataset("postgres://postgres_default/airflow/staging/order_with_merchant")
req8 = Dataset("postgres://postgres_default/airflow/dw/dim_staff")
req9 = Dataset("postgres://postgres_default/airflow/dw/dim_merchant")

waiting_for = [req1,
               req2,
               req3,
               req4,
               req5,
               req6,
               req7,
               req8,
               req9,
               ]
sql_file_name = r"/app/sql/fact_transaction.sql"

dag = create_to_dw_dag(
    source_name = "fact_transaction",
    schedule = waiting_for,
    sql_file_name = sql_file_name
)