FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/business_dept/business_dept_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "business_dept_ingest.py" ]