from datetime import datetime
import cloudscraper
import pandas.io.sql as sqlio
from db_connection import get_db_connection


ETL_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
DB_LIST = ['tv', 'drama', 'movie']

for db_name in DB_LIST:
    try:
        scraper = cloudscraper.create_scraper()
        conn = get_db_connection(system_name="MDL")
        cur = conn.cursor()

        fetch_query = f"""
            select * from public.{db_name} 
            where DATE_PART('day', localtimestamp(0) - update_ts) > 30;
        """
        df = sqlio.read_sql_query(fetch_query, conn)
        print(f"{ETL_TIME} INFO: Fetched {len(df)} records for {db_name} DB")

        id_list = []
        for index, row in df.iterrows():
            response = scraper.get(row["url"])
            if response.status_code == 404:
                id_list.append(str(row["id"]))
        if len(id_list) != 0:
            update_query = f"""
            update 
                public.{db_name} 
            set 
                ranking = 999999, 
                released_at = '2099-12-31', 
                rating = 0, 
                update_ts = '{ETL_TIME}'
            where 
                id in ({','.join(id_list)});
            """
            cur.execute(update_query)
            conn.commit()
            print(f"{ETL_TIME} INFO: {len(id_list)} records are updated for {db_name} DB")
        else:
            print(f"{ETL_TIME} INFO: No record to update for {db_name} DB")
    except Exception as e:
        print(f"{ETL_TIME} ERROR: {str(e)}")

    finally:
        cur.close()
        conn.close()
