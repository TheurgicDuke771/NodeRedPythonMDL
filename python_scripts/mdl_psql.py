import warnings
from datetime import datetime
import requests
import pandas as pd
from db_connection import get_db_connection


warnings.filterwarnings("ignore")
ETL_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_synopsis(item_url: str) -> str:
    details_url = f"https://kuryana.vercel.app/id{item_url}"
    try:
        response_raw = requests.request("GET", details_url)
        if response_raw.status_code == 200:
            response = response_raw.json()
            synopsis_str = str(response["data"]["synopsis"])
            synopsis = synopsis_str.replace("'", "''") if synopsis_str != "" else "No synopsis available"
            return synopsis
        else:
            raise Exception(f"URL: {details_url}. Response: {response_raw.text}")
    except Exception as e:
        print(f"{ETL_TIME} WARNING: {str(e)}")
        return "No synopsis available"


def inset_into_db(content_list: list) -> int:
    affected_rec_cnt = 0

    try:
        conn = get_db_connection(system_name="MDL")
        cur = conn.cursor()

        for item in content_list:
            try:
                try:
                    item_id = item["id"]
                except:
                    raise Exception(f"ID Not Found. {item}")
                try:
                    title = item["title"].replace("'", "''")
                except:
                    title = "No working Title"
                try:
                    episodes = item["episodes"]
                except:
                    episodes = 0
                try:
                    ranking = item["ranking"]
                except:
                    ranking = 99999
                try:
                    popularity = item["popularity"]
                except:
                    popularity = 99999
                try:
                    country = item["country"]
                except:
                    country = "Country Name Not Found"
                try:
                    content_type = item["content_type"]
                except:
                    content_type = "Content Type Not Found"
                try:
                    item_type = item["type"]
                except:
                    raise Exception(f"Type Not Found. {item}")
                try:
                    datetime.strptime(item["released_at"], "%Y-%m-%d")
                    released_at = item["released_at"]
                except:
                    released_at = "2099-12-31"
                try:
                    url = f"https://mydramalist.com{item['url']}"
                except:
                    raise Exception(f"URL Not Found. {item}")
                try:
                    genres = item["genres"]
                except:
                    genres = "Dfault"
                try:
                    thumbnail = item["thumbnail"]
                except:
                    thumbnail = "https://i.mydramalist.com/_4t.jpg"
                try:
                    cover = item["cover"]
                except:
                    cover = "https://i.mydramalist.com/_4c.jpg"
                try:
                    rating = item["rating"]
                except:
                    rating = 99999

                table_name = "movie" if item_type == "Movie" else "tv"

                fetch_query = f'SELECT * FROM {table_name} WHERE "id" = {item_id};'
                result = pd.read_sql(fetch_query, conn)

                if len(result) == 0:
                    try:
                        synopsis = get_synopsis(item_url=item["url"])
                    except:
                        synopsis = "No synopsis available"

                    insert_query = f"""
                    INSERT INTO {table_name} 
                    ("id", "title", "episodes", "ranking", 
                    "best_ranking", "popularity", "best_popularity", 
                    "country", "content_type", "type", "synopsis",
                    "released_at", "url", "genres", "thumbnail",
                    "cover", "rating", "best_rating", "insert_ts", "update_ts") 
                    VALUES 
                    ({item_id}, '{title}', {episodes}, {ranking}, {ranking},
                    {popularity}, {popularity}, '{country}', '{content_type}',
                    '{item_type}', '{synopsis}', '{released_at}', '{url}', '{genres}',
                    '{thumbnail}', '{cover}', {rating}, {rating}, '{ETL_TIME}', '{ETL_TIME}');
                    """

                    cur.execute(insert_query)
                    conn.commit()
                    affected_rec_cnt += 1

                else:
                    existing_synopsis = result["synopsis"].values[0]
                    if (
                        (existing_synopsis == "No synopsis available")
                        or (existing_synopsis[-3:] == "...")
                        or (existing_synopsis == "Remove ads")
                    ):
                        try:
                            synopsis = get_synopsis(item_url=item["url"])
                        except:
                            synopsis = "No synopsis available"

                        update_query = f"""
                        UPDATE {table_name} SET 
                        "title" = '{title}',
                        "episodes" = {episodes}, 
                        "ranking" = {ranking}, 
                        "best_ranking" = case when ("best_ranking" > {ranking}) then {ranking} else "best_ranking" end,
                        "popularity" = {popularity}, 
                        "best_popularity" = case when ("best_popularity" > {popularity}) then {popularity} else "best_popularity" end,
                        "country" = '{country}',
                        "content_type" = '{content_type}', 
                        "type" = '{item_type}',
                        "synopsis" = '{synopsis}', 
                        "released_at" = '{released_at}', 
                        "url" = '{url}', 
                        "genres" = '{genres}', 
                        "thumbnail" = '{thumbnail}', 
                        "cover" = '{cover}', 
                        "rating" = {rating}, 
                        "best_rating" = case when ("best_rating" < {rating}) then {rating} else "best_rating" end,
                        "update_ts" = '{ETL_TIME}'
                        WHERE "id" = {item_id};
                        """
                        cur.execute(update_query)
                        conn.commit()
                        affected_rec_cnt += 1

                    else:
                        update_query = f"""
                        UPDATE {table_name} SET 
                        "title" = '{title}',
                        "episodes" = {episodes}, 
                        "ranking" = {ranking}, 
                        "best_ranking" = case when ("best_ranking" > {ranking}) then {ranking} else "best_ranking" end,
                        "popularity" = {popularity}, 
                        "best_popularity" = case when ("best_popularity" > {popularity}) then {popularity} else "best_popularity" end,
                        "country" = '{country}',
                        "content_type" = '{content_type}', 
                        "type" = '{item_type}',
                        "released_at" = '{released_at}', 
                        "url" = '{url}', 
                        "genres" = '{genres}', 
                        "thumbnail" = '{thumbnail}', 
                        "cover" = '{cover}', 
                        "rating" = {rating}, 
                        "best_rating" = case when ("best_rating" < {rating}) then {rating} else "best_rating" end,
                        "update_ts" = '{ETL_TIME}'
                        WHERE "id" = {item_id};
                        """
                        cur.execute(update_query)
                        conn.commit()
                        affected_rec_cnt += 1
            except Exception as e:
                print(f"{ETL_TIME} WARNING: {str(e)}")

        return affected_rec_cnt

    except Exception as e:
        print(f"{ETL_TIME} ERROR: {str(e)}")
        return affected_rec_cnt

    finally:
        cur.close()
        conn.close()
