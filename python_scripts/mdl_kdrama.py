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

df["title"] = '<a href="https://mydramalist.com' + df["url"] + '">' + df["title"] + "</a>"
k_df = df[(df["country"] == "South Korea") & (df["type"] != "Movie")].sort_values(by=["ranking"]).reset_index()

print(k_df[["title", "episodes", "ranking", "genres", "rating"]].head(10).to_json(orient="records"))
scraper.close()
