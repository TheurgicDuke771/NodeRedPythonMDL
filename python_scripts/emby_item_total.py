import json
from math import ceil
import pandas as pd
from db_connection import get_db_connection


try:
    conn = get_db_connection(system_name="EMBY")

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
