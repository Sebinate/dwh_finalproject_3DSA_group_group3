FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/business_dept/product_list/product_list_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "product_list_ingest.py" ]