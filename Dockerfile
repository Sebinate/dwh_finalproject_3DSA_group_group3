FROM apache/airflow:2.9.1

USER airflow

RUN pip install --no-cache-dir \
    python-dotenv \
    apache-airflow-providers-docker