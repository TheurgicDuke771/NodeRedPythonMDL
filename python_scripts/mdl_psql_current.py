import cloudscraper
from datetime import datetime
from .mdl_psql import inset_into_db


now = datetime.now()
current_year = now.year
current_quarter = ((now.month - 1) // 3) + 1

scraper = cloudscraper.create_scraper()

seasonal = scraper.post(
    url="https://mydramalist.com/v1/quarter_calendar",
    data={"quarter": current_quarter, "year": current_year},
)
mdl_list = seasonal.json()

scraper.close()
rec_count = inset_into_db(mdl_list)
print(f'{rec_count} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')
