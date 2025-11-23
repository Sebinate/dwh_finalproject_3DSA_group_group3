FROM python:3.11-slim

WORKDIR /app
RUN pip install psycopg2-binary sqlalchemy

COPY infra/transform_to_dw/ /app/infra/transsform_to_dw/
COPY scripts/utils/utils.py /app/scripts/utils/utils.py

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD [ "echo", "This is the Ingestion Service. Specify a script to run" ]