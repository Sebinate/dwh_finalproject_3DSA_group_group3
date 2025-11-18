FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/utils/ /app/scripts/utils

COPY infra/ingestion/customer_dept/user_credit_card/user_credit_card_ingest.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "python", "user_credit_card.py" ]