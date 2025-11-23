import os
import sys

from sqlalchemy import text
from scripts.utils import utils

sql_file = os.environ["SQL_FILE"]

def main():
    try:
        engine = utils.connect()

        path = os.path.join("sql/", sql_file)

        with open(path, "rb") as file:
            sql_commands = file.read()

        with engine.connect() as conn:
            with conn.begin():
                queries = sql_commands.split(";")

                for query in queries:
                    query = query.strip()

                    if query:
                        conn.execute(text(query))
    except FileNotFoundError:
        print("File not found")
        sys.exit(1)

    except Exception as e:
        print(f"Error Executing SQL: {e}")
        sys.exit(1)

main()