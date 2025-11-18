FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/marketing_dept/transactional_campaign_data/transactional_campaign_data_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "transactional_campaign_data_ingest.py" ]