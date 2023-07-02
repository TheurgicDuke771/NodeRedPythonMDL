from datetime import datetime
import cloudscraper
from mdl_psql import inset_into_db


now = datetime.now()
current_year = now.year
current_quarter = ((now.month - 1) // 3) + 1
mdl_list = []

try:
    scraper = cloudscraper.create_scraper()
    for year in range(2022, current_year + 1):
        if year == current_year:
            for q in range(1, current_quarter + 1):
                current_seasonal = scraper.post(
                    url="https://mydramalist.com/v1/quarter_calendar",
                    data={"quarter": q, "year": year},
                )
                temp_list = current_seasonal.json()
                mdl_list.extend(temp_list)
        else:
            for q in range(1, 5):
                history_seasonal = scraper.post(
                    url="https://mydramalist.com/v1/quarter_calendar",
                    data={"quarter": q, "year": year},
                )
                temp_list = history_seasonal.json()
                mdl_list.extend(temp_list)
except Exception as e:
    print(f"{datetime.now()} ERROR: {str(e)}")
finally:
    scraper.close()

if len(mdl_list) > 0:
    rec_count = inset_into_db(mdl_list)
    print(f'{datetime.now()} INFO: {rec_count} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')
else:
    print(f"{datetime.now()} INFO: No records to insert")
