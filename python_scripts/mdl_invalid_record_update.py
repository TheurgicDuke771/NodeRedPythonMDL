from datetime import datetime
import requests
import pandas.io.sql as sqlio
from db_connection import get_db_connection


try:
    conn = get_db_connection(system_name="MDL")
    cur = conn.cursor()

    fetch_query = "select * from public.tv where DATE_PART('day', localtimestamp(0) - update_ts) > 30;"
    df = sqlio.read_sql_query(fetch_query, conn)
    print(f"Fetched {len(df)} records")

    id_list = []
    for index, row in df.iterrows():
        response = requests.request("GET", row["url"])
        if response.status_code == 404:
            id_list.append(str(row["id"]))
    if len(id_list) != 0:
        update_query = f"""
        update 
            public.tv 
        set 
            ranking = 99999, 
            released_at = '2099-12-31', 
            rating = 0, 
            update_ts = '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        where 
            id in ({','.join(id_list)});
        """
        cur.execute(update_query)
        conn.commit()
        print(f"{len(id_list)} records are updated")
    else:
        print("No record to update")

except Exception as e:
    print(str(e))

finally:
    cur.close()
    conn.close()
