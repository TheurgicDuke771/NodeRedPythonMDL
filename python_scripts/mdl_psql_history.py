from datetime import datetime
import cloudscraper
from mdl_psql import inset_into_db


now = datetime.now()
current_year = now.year
current_quarter = ((now.month - 1) // 3) + 1
mdl_list = []
ETL_TIME = now.strftime("%Y-%m-%d %H:%M:%S")

try:
    scraper = cloudscraper.create_scraper()
    for year in range((current_year - 2), (current_year + 1)):
        if year == current_year:
            for q in range(1, current_quarter + 1):
                current_seasonal = scraper.post(
                    url="https://mydramalist.com/v1/quarter_calendar",
                    data={"quarter": q, "year": year},
                )
                if current_seasonal.status_code == 200:
                    temp_list = current_seasonal.json()
                    mdl_list.extend(temp_list)
                else:
                    raise Exception(f"URL: {current_seasonal.url}. Response: {current_seasonal.text}")
        else:
            for q in range(1, 5):
                history_seasonal = scraper.post(
                    url="https://mydramalist.com/v1/quarter_calendar",
                    data={"quarter": q, "year": year},
                )
                if history_seasonal.status_code == 200:
                    temp_list = history_seasonal.json()
                    mdl_list.extend(temp_list)
                else:
                    raise Exception(f"URL: {history_seasonal.url}. Response: {history_seasonal.text}")
except Exception as e:
    print(f"{ETL_TIME} ERROR: {str(e)}")
finally:
    scraper.close()

if len(mdl_list) > 0:
    rec_count = inset_into_db(mdl_list)
    print(f'{ETL_TIME} INFO: {rec_count} records inserted at {ETL_TIME}.')
else:
    print(f"{ETL_TIME} INFO: No records to insert")
