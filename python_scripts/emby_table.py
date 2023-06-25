import pandas as pd
from db_connection import get_db_connection


try:
    conn = get_db_connection(system_name="EMBY")

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
