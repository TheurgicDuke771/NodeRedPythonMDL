import psycopg2
import requests
from datetime import datetime


now = datetime.now()
current_year = now.year
current_quarter = ((now.month-1)//3)+1

reqUrl = f"https://kuryana.vercel.app/seasonal/{current_year}/{current_quarter}"
response = requests.request("GET", reqUrl)
mdl_list = response.json()

try:
	conn = psycopg2.connect(
		user="xxxxx", 
		password="xxxxx", 
		host="xxxxx", 
		port="xxxxx",
		dbname="xxxxx")

	cur = conn.cursor()

	for item in mdl_list:
		id = item['id']
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
		type = item['type']
		try:
			synopsis = item['synopsis'].replace("'", "''")
		except:
			synopsis = 'No synopsis available'
		try:
			datetime.strptime(item['released_at'], "%Y-%m-%d")
			released_at = item['released_at']
		except:
			released_at = '2099-12-31'
		url = "https://mydramalist.com"+item['url']
		genres = item['genres']
		thumbnail = item['thumbnail']
		cover = item['cover']
		try:
			rating = item['rating']
		except:
			rating = 99999

		table_name = 'movie' if type == 'Movie' else 'tv'

		insert_query = f'''
		INSERT INTO {table_name} 
		("id", "title", "episodes", "ranking", "popularity",
		"country", "content_type", "type", "synopsis",
		"released_at", "url", "genres", "thumbnail",
		"cover", "rating", "insert_ts", "update_ts") 
		VALUES 
		({id}, '{title}', {episodes}, {ranking}, {popularity},
		'{country}', '{content_type}', '{type}', '{synopsis}',
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
		"type" = '{type}',
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
		
	print(f'{len(mdl_list)} records inserted at {now.strftime("%Y-%m-%d %H:%M:%S")}.')

except Exception as e:
	print(e)

finally:
	cur.close()
	conn.close()

