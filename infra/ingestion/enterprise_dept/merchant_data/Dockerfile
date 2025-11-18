FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/enterprise_dept/merchant_data/merchant_data_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "merchant_data_ingest.py" ]