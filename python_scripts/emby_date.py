import os
import json
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
	SELECT to_char("datecreated", 'YYYY-MM-DD') AS "date_played",
	((SUM(playduration) - SUM(pauseduration))/60) AS playtime
	FROM playbackactivity
	WHERE "datecreated" :: date > (NOW() - INTERVAL '15 DAY')
	GROUP BY date_played
	ORDER BY date_played DESC
	"""
    cur.execute(fetch_query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["date", "playtime"])
    df["playtime"] = df["playtime"].astype(int)

    chart = [{"series": ["Daily Play Time"], "data": [df["playtime"].to_list()], "labels": df["date"].tolist()}]

    print(json.dumps(chart))


except Exception as e:
    print(str(e))

finally:
    cur.close()
    conn.close()
