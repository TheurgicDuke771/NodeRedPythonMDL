import os
import json
from math import ceil
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
    SELECT itemtype,
    ((SUM(playduration) - SUM(pauseduration))/3600) AS playtime
    FROM playbackactivity
    GROUP BY itemtype
    """
    cur.execute(fetch_query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["itemtype", "playtime"])

    chart = [
        {
            "series": ["Total Play Time"],
            "data": [[ceil(x) for x in df["playtime"].to_list()]],
            "labels": df["itemtype"].to_list(),
        }
    ]

    print(json.dumps(chart))

except Exception as e:
    print(str(e))

finally:
    cur.close()
    conn.close()
