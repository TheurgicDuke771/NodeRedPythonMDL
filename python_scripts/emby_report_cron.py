import os
import json
import psycopg2
import requests
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

print(datetime.now(), " [LOG]:")
emby_ip = os.getenv("EMBY_IP")

auth_url = f"{emby_ip}/Users/AuthenticateByName"
payload = json.dumps({"Username": os.getenv("EMBY_USER_NAME"), "pw": os.getenv("EMBY_PASSWORD")})
headers = {
    "Authorization": f'''Emby UserId="{os.getenv('EMBY_USER_ID')}",Client="Python", Device="Raspberry Pi 4B", DeviceId="xxx", Version="3.9"''',
    "Content-Type": "application/json",
}
auth_response = requests.request("POST", auth_url, headers=headers, data=payload)

token = auth_response.json()["AccessToken"]
query_string = "SELECT * FROM PlaybackActivity"
query_url = f"{emby_ip}/emby/user_usage_stats/submit_custom_query?X-Emby-Token={token}&CustomQueryString={query_string}&ReplaceUserId=True"

query_response = requests.request("POST", query_url)
resp = query_response.json()
cols = resp["colums"]
qry_result = resp["results"]

cols = [item.lower() for item in cols]
joined_cols = '","'.join(cols)

try:
    if len(qry_result) == 0:
        raise Exception("No records present")

    conn = psycopg2.connect(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("EMBY_DB_NAME"),
    )

    print("\tConnected to DB EmbyReporting")

    cur = conn.cursor()
    truncate_query = "TRUNCATE TABLE playbackactivity;"
    cur.execute(truncate_query)
    conn.commit()
    print("\tCleared Table PlaybackActivity")

    for rec in qry_result:
        if len(rec) != 11:
            raise Exception("Data missing in record")

        insert_query = f"""
        INSERT INTO playbackactivity ("{joined_cols}") 
        VALUES 
        ('{rec[0]}', '{rec[1]}', {rec[2]}, '{rec[3]}', '{rec[4].replace("'", "''")}', '{rec[5]}', 
        '{rec[6]}', '{rec[7].replace("'", "''")}', {rec[8]}, {rec[9]}, '{rec[10]}')
        """
        cur.execute(insert_query)
        conn.commit()
    print(f"\t{len(qry_result)} records inserted into PlaybackActivity.")

except Exception as e:
    print("\t", str(e))

finally:
    cur.close()
    conn.close()
