from datetime import datetime
from mdl_psql import inset_into_db
from mdl_seasonal_data import get_seasonal_data


ETL_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
try:
    mdl_list = get_seasonal_data()
    if len(mdl_list) > 0:
        rec_count = inset_into_db(mdl_list)
        print(f"{ETL_TIME} INFO: {rec_count} records inserted.")
    else:
        print(f"{ETL_TIME} INFO: No records to insert")
except Exception as e:
    print(f"{ETL_TIME} ERROR: {str(e)}")
