import os
import json
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

def inset_into_db(content_list:list) -> int:
	now = datetime.now()
	try:
		conn = psycopg2.connect(
			user=os.getenv('PG_USER'), 
			password=os.getenv('PG_PASSWORD'), 
			host=os.getenv('PG_HOST'), 
			port=os.getenv('PG_PORT'),
			dbname=os.getenv('MDL_DB_NAME'))

		cur = conn.cursor()

		for item in content_list:
			try:
				id = item['id']
			except:
				raise Exception("ID Not Found")
			title = item['title'].replace("'", "''")
			try:
				episodes = item['episodes']
			except:
				episodes = 0
			try:
				ranking = item['ranking']
			except:
				ranking = 99999
			try:
				popularity = item['popularity']
			except:
				popularity = 99999
			try:
				country = item['country']
			except:
				country = 'Country Name Not Found'
			try:
				content_type = item['content_type']
			except:
				content_type = 'Content Type Not Found'
			try:
				c_type = item['type']
			except:
				raise Exception("Type Not Found")
			try:
				details_url = f"https://kuryana.vercel.app/id{item['url']}"
				response = requests.request("GET", details_url).json()
				synopsis_str = str(response['data']['synopsis'])
				synopsis = synopsis_str.replace("'", "''") if synopsis_str != '' else 'No synopsis available'
			except:
				synopsis = 'No synopsis available'
			try:
				datetime.strptime(item['released_at'], "%Y-%m-%d")
				released_at = item['released_at']
			except:
				released_at = '2099-12-31'
			url = f"https://mydramalist.com{item['url']}"
			genres = item['genres']
			thumbnail = item['thumbnail']
			cover = item['cover']
			try:
				rating = item['rating']
			except:
				rating = 99999

			table_name = 'movie' if c_type == 'Movie' else 'tv'

			insert_query = f'''
			INSERT INTO {table_name} 
			("id", "title", "episodes", "ranking", "popularity",
			"country", "content_type", "type", "synopsis",
			"released_at", "url", "genres", "thumbnail",
			"cover", "rating", "insert_ts", "update_ts") 
			VALUES 
			({id}, '{title}', {episodes}, {ranking}, {popularity},
			'{country}', '{content_type}', '{c_type}', '{synopsis}',
			'{released_at}', '{url}', '{genres}', '{thumbnail}',
			'{cover}', {rating}, '{now.strftime("%Y-%m-%d %H:%M:%S")}',
			'{now.strftime("%Y-%m-%d %H:%M:%S")}');
			'''
			update_query = f'''
			UPDATE {table_name} SET 
			"title" = '{title}',
			"episodes" = {episodes}, 
			"ranking" = {ranking}, 
			"popularity" = {popularity}, 
			"country" = '{country}',
			"content_type" = '{content_type}', 
			"type" = '{c_type}',
			"synopsis" = '{synopsis}', 
			"released_at" = '{released_at}', 
			"url" = '{url}', 
			"genres" = '{genres}', 
			"thumbnail" = '{thumbnail}', 
			"cover" = '{cover}', 
			"rating" = {rating}, 
			"update_ts" = '{now.strftime("%Y-%m-%d %H:%M:%S")}'
			WHERE "id" = {id};
			'''
			fetch_query = f'SELECT * FROM {table_name} WHERE "id" = {id};'

			cur.execute(fetch_query)
			result = cur.fetchall()
			if len(result) == 0:
				cur.execute(insert_query)
				conn.commit()
			else:
				cur.execute(update_query)
				conn.commit()
		return len(content_list)

	except Exception as e:
		print(e)

	finally:
		cur.close()
		conn.close()
