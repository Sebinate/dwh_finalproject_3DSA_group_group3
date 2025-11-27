from sqlalchemy import create_engine, Engine
import os

def connect() -> Engine:
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT")
    db = os.environ.get("DB_NAME")

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    return create_engine(connection_string)