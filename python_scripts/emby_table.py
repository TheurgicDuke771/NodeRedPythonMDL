import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

try:
    conn = psycopg2.connect(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("EMBY_DB_NAME"),
    )

    cur = conn.cursor()
    fetch_query = """
    SELECT to_char("datecreated", 'DD HH12:MI AM') AS "date", "itemname", "devicename" 
    FROM playbackactivity ORDER BY "datecreated" DESC LIMIT 15;
    """
    cur.execute(fetch_query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["date", "itemname", "devicename"])
    print(df.to_json(orient="records"))

except Exception as e:
    print(str(e))

finally:
    cur.close()
    conn.close()
