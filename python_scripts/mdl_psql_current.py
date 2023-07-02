from datetime import datetime
from mdl_psql import inset_into_db
from mdl_seasonal_data import get_seasonal_data


now = datetime.now()
mdl_list = get_seasonal_data()
if len(mdl_list) > 0:
    rec_count = inset_into_db(mdl_list)
    print(f'{datetime.now()} INFO: {rec_count} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')
else:
    print(f"{datetime.now()} INFO: No records to insert")
