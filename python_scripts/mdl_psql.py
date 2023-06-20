import os
import json
import requests
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import warnings


warnings.filterwarnings('ignore')
load_dotenv()


def get_synopsis(item_url:str) -> str:
	details_url = f"https://kuryana.vercel.app/id{item_url}"
	response = requests.request("GET", details_url).json()
	synopsis_str = str(response['data']['synopsis'])
	synopsis = synopsis_str.replace("'", "''") if synopsis_str != '' else 'No synopsis available'
	return synopsis


def inset_into_db(content_list:list) -> int:
	affected_rec_cnt = 0
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
			try:
				title = item['title'].replace("'", "''")
			except:
				title = 'No working Title'
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

			fetch_query = f'SELECT * FROM {table_name} WHERE "id" = {id};'
			result = pd.read_sql(fetch_query, conn)
			
			if len(result) == 0:
				try:
					synopsis = get_synopsis(item_url=item['url'])
				except:
					synopsis = 'No synopsis available'
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
				
				cur.execute(insert_query)
				conn.commit()
				affected_rec_cnt += 1
			else:
				existing_synopsis = result['synopsis'].values[0]
				if (existing_synopsis == 'No synopsis available') or (existing_synopsis[-3:] == '...'):
					try:
						synopsis = get_synopsis(item_url=item['url'])
					except:
						synopsis = 'No synopsis available'
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
					cur.execute(update_query)
					conn.commit()
					affected_rec_cnt += 1
				else:
					update_query = f'''
					UPDATE {table_name} SET 
					"title" = '{title}',
					"episodes" = {episodes}, 
					"ranking" = {ranking}, 
					"popularity" = {popularity}, 
					"country" = '{country}',
					"content_type" = '{content_type}', 
					"type" = '{c_type}',
					"released_at" = '{released_at}', 
					"url" = '{url}', 
					"genres" = '{genres}', 
					"thumbnail" = '{thumbnail}', 
					"cover" = '{cover}', 
					"rating" = {rating}, 
					"update_ts" = '{now.strftime("%Y-%m-%d %H:%M:%S")}'
					WHERE "id" = {id};
					'''
					cur.execute(update_query)
					conn.commit()
					affected_rec_cnt += 1
			
		return affected_rec_cnt

	except Exception as e:
		print(e)
  		return affected_rec_cnt

	finally:
		cur.close()
		conn.close()
