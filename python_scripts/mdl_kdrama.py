import pandas as pd
from mdl_seasonal_data import get_seasonal_data


seasonal = get_seasonal_data()
df = pd.DataFrame(seasonal)

df["title"] = '<a href="https://mydramalist.com' + df["url"] + '">' + df["title"] + "</a>"
k_df = df[(df["country"] == "South Korea") & (df["type"] != "Movie")].sort_values(by=["ranking"]).reset_index()

print(k_df[["title", "episodes", "ranking", "genres", "rating"]].head(10).to_json(orient="records"))
