import json
import cloudscraper
import pandas as pd
from datetime import datetime


now = datetime.now()
current_year = now.year
current_quarter = ((now.month - 1) // 3) + 1

scraper = cloudscraper.create_scraper()

seasonal = scraper.post(
    url="https://mydramalist.com/v1/quarter_calendar",
    data={"quarter": current_quarter, "year": current_year},
).json()

df = pd.DataFrame(seasonal)

top_hundred = df[df["type"] != "Movie"].sort_values(by=["ranking"]).head(100).reset_index()
title_count = top_hundred[["title", "country"]].groupby(["country"]).count().reset_index()

chart = [
    {"series": ["No Of Dramas"], "data": [title_count["title"].to_list()], "labels": title_count["country"].to_list()}
]

print(json.dumps(chart))
scraper.close()
