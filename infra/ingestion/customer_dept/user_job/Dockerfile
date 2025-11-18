FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/customer_dept/user_job/user_job_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "user_job_ingest.py" ]