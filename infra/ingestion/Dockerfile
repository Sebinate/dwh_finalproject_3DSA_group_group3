FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY infra/ingestion/ /app/infra/ingestion/
COPY scripts/utils/ /app/scripts/utils/

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "echo", "This is the Ingestion Service. Specify a script to run" ]