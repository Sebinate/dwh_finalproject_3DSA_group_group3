FROM python:3.11-slim

WORKDIR /app
COPY infra/ml/requirements.txt .
RUN pip install -r requirements.txt

COPY infra/ml/ /app/infra/ml/
COPY scripts/utils/ /app/scripts/utils/

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "echo", "This is the Ingestion Service. Specify a script to run" ]