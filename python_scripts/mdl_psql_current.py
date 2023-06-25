from datetime import datetime
from mdl_psql import inset_into_db
from mdl_seasonal_data import get_seasonal_data


now = datetime.now()
mdl_list = get_seasonal_data()
rec_count = inset_into_db(mdl_list)
print(f'{rec_count} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')
