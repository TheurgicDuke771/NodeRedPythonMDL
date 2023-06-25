import json
import pandas as pd
from mdl_seasonal_data import get_seasonal_data


seasonal = get_seasonal_data()
df = pd.DataFrame(seasonal)

top_hundred = df[df["type"] != "Movie"].sort_values(by=["ranking"]).head(100).reset_index()
title_count = top_hundred[["title", "country"]].groupby(["country"]).count().reset_index()

chart = [
    {"series": ["No Of Dramas"], "data": [title_count["title"].to_list()], "labels": title_count["country"].to_list()}
]

print(json.dumps(chart))
