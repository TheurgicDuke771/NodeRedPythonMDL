from datetime import datetime
import requests
import pandas as pd
import json


now = datetime.now()
current_year = now.year
current_quarter = ((now.month-1)//3)+1

reqUrl = f"https://kuryana.vercel.app/seasonal/{current_year}/{current_quarter}"

response = requests.request("GET", reqUrl)
df = pd.DataFrame(response.json())

top_hundred = df[df['type'] != 'Movie'].sort_values(by=['ranking']).head(100).reset_index()
title_count = top_hundred[['title', 'country']].groupby(['country']).count().reset_index()

chart = [{
    "series": [ "No Of Dramas" ],
    "data": [ title_count['title'].to_list() ],
    "labels": title_count['country'].to_list()
}]

print(json.dumps(chart))

