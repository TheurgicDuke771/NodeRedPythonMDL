from datetime import datetime
import requests
import pandas as pd


now = datetime.now()
current_year = now.year
current_quarter = ((now.month-1)//3)+1

reqUrl = f"https://kuryana.vercel.app/seasonal/{current_year}/{current_quarter}"

response = requests.request("GET", reqUrl)
df = pd.DataFrame(response.json())

c_df = df[(df['country'] == 'China') & (df['type'] != 'Movie')].sort_values(by=['ranking']).reset_index()

print(c_df[['title', 'episodes', 'ranking', 'genres','rating']].head(15).to_json(orient='records'))


