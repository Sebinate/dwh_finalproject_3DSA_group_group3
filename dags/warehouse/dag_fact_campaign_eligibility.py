from airflow.datasets import Dataset
from warehouse.builder import create_to_dw_dag

req1 = Dataset("postgres://postgres_default/airflow/dw/dim_campaign")
req2 = Dataset("postgres://postgres_default/airflow/dw/dim_order")
req3 = Dataset("postgres://postgres_default/airflow/staging/transactional_campaign_data")

waiting_for = [req1, req2, req3]

sql_file_name = r"/app/sql/fact_promotion_eligibility.sql"

dag = create_to_dw_dag(
    source_name = "fact_promotion_eligibility",
    schedule = waiting_for,
    sql_file_name = sql_file_name
)