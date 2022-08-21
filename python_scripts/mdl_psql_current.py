import requests
from datetime import datetime
from mdl_psql import inset_into_db


now = datetime.now()
current_year = now.year
current_quarter = ((now.month-1)//3)+1

reqUrl = f"https://kuryana.vercel.app/seasonal/{current_year}/{current_quarter}"
response = requests.request("GET", reqUrl)
mdl_list = response.json()

rec_count = inset_into_db(mdl_list)
print(f'{rec_count} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')
