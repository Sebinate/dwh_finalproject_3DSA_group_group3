FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/operations_dept/line_item_prices/line_item_prices_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "line_item_prices_ingest.py" ]