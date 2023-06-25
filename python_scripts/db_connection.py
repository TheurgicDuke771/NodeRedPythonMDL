import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()


def get_db_connection(system_name: str) -> psycopg2.connect:
    if system_name.upper() == "MDL":
        db_name = os.getenv("MDL_DB_NAME")
    elif system_name.upper() == "EMBY":
        db_name = os.getenv("EMBY_DB_NAME")
    else:
        db_name = None
        raise Exception("Unknown Database Name")

    conn = psycopg2.connect(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=db_name,
    )
    return conn
