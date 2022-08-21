import requests
from datetime import datetime
from mdl_psql import inset_into_db


now = datetime.now()
current_year = now.year
current_quarter = ((now.month-1)//3)+1
mdl_list = []

for year in range(2022, current_year+1):
	if year == current_year:
		for q in range(1, current_quarter+1):
			reqUrl = f"https://kuryana.vercel.app/seasonal/{year}/{q}"
			response = requests.request("GET", reqUrl)
			temp_list = response.json()
			mdl_list.extend(temp_list)
	else:
		for q in range(1, 5):
			reqUrl = f"https://kuryana.vercel.app/seasonal/{year}/{q}"
			response = requests.request("GET", reqUrl)
			temp_list = response.json()
			mdl_list.extend(temp_list)

rec_count = inset_into_db(mdl_list)
print(f'{rec_count} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')
