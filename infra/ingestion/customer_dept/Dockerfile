FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/customer_dept/customer_management_dept.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "customer_management_dept.py" ]