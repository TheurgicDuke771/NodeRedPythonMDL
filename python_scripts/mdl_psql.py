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
        with conn, conn.cursor() as cur:
            for item in content_list:
                try:
                    item_id = item.get("id")
                    if item_id is None:
                        raise Exception(f"ID Not Found. {item}")

                    title = item.get("title", "No working Title").replace("'", "''")
                    episodes = item.get("episodes", 0)
                    ranking = item.get("ranking", 999999)
                    popularity = item.get("popularity", 999999)
                    country = item.get("country", "Country Name Not Found")
                    content_type = item.get("content_type", "Content Type Not Found")
                    item_type = item.get("type")
                    if item_type is None:
                        raise Exception(f"Type Not Found. {item}")

                    released_at = item.get("released_at", "2099-12-31")
                    try:
                        datetime.strptime(released_at, "%Y-%m-%d")
                    except Exception:
                        released_at = "2099-12-31"

                    url = f"https://mydramalist.com{item.get('url', '')}"
                    genres = item.get("genres", "Default")
                    thumbnail = item.get("thumbnail", "https://i.mydramalist.com/_4t.jpg")
                    cover = item.get("cover", "https://i.mydramalist.com/_4c.jpg")
                    rating = item.get("rating", 999999)

                    table_name = (
                        "movie" if item_type == "Movie"
                        else "drama" if item_type in ("Drama", "Special")
                        else "tv"
                    )

                    fetch_query = f'SELECT * FROM {table_name} WHERE "id" = %s;'
                    result = pd.read_sql(fetch_query, conn, params=(item_id,))

                    if len(result) == 0:
                        synopsis = get_synopsis(item.get("url", "")) or "No synopsis available"
                        insert_query = f"""
                        INSERT INTO {table_name} 
                        ("id", "title", "episodes", "ranking", 
                        "best_ranking", "popularity", "best_popularity", 
                        "country", "content_type", "record_type", "synopsis",
                        "released_at", "url", "genres", "thumbnail",
                        "cover", "rating", "best_rating", "insert_ts", "update_ts") 
                        VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        cur.execute(insert_query, (
                            item_id, title, episodes, ranking, ranking, popularity, popularity,
                            country, content_type, item_type, synopsis, released_at, url, genres,
                            thumbnail, cover, rating, rating, ETL_TIME, ETL_TIME
                        ))
                        affected_rec_cnt += 1
                    else:
                        existing_synopsis = result["synopsis"].values[0]
                        needs_synopsis = (
                            existing_synopsis == "No synopsis available"
                            or existing_synopsis[-3:] == "..."
                            or existing_synopsis == "Remove ads"
                        )
                        synopsis = get_synopsis(item.get("url", "")) if needs_synopsis else existing_synopsis
                        update_query = f"""
                        UPDATE {table_name} SET 
                        "title" = %s,
                        "episodes" = %s, 
                        "ranking" = %s, 
                        "best_ranking" = case when ("best_ranking" > %s) then %s else "best_ranking" end,
                        "popularity" = %s, 
                        "best_popularity" = case when ("best_popularity" > %s) then %s else "best_popularity" end,
                        "country" = %s,
                        "content_type" = %s, 
                        "record_type" = %s,
                        "synopsis" = %s, 
                        "released_at" = %s, 
                        "url" = %s, 
                        "genres" = %s, 
                        "thumbnail" = %s, 
                        "cover" = %s, 
                        "rating" = %s, 
                        "best_rating" = case when ("best_rating" < %s) then %s else "best_rating" end,
                        "update_ts" = %s
                        WHERE "id" = %s;
                        """
                        cur.execute(update_query, (
                            title, episodes, ranking, ranking, ranking, popularity, popularity, popularity,
                            country, content_type, item_type, synopsis, released_at, url, genres,
                            thumbnail, cover, rating, rating, rating, ETL_TIME, item_id
                        ))
                        affected_rec_cnt += 1
                except Exception as e:
                    print(f"{ETL_TIME} WARNING: {str(e)}")
            conn.commit()
        return affected_rec_cnt

    except Exception as e:
        print(f"{ETL_TIME} ERROR: {str(e)}")
        return affected_rec_cnt
