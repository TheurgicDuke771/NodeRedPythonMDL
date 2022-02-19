import json
from math import ceil
import psycopg2
import pandas as pd


try:	
	conn = psycopg2.connect(
		user="postgres", 
		password="postgres", 
		host="localhost", 
		port="5432",
		dbname="EmbyReporting")

	cur = conn.cursor()
	fetch_query = '''
	SELECT itemtype,
	((SUM(playduration) - SUM(pauseduration))/3600) AS playtime
	FROM playbackactivity
	GROUP BY itemtype
	'''
	cur.execute(fetch_query)
	rows = cur.fetchall()
	df = pd.DataFrame(rows, columns=['itemtype', 'playtime'])
	
	chart = [{
		"series": [ "Total Play Time" ],
		"data": [ [ceil(x) for x in df['playtime'].to_list()] ],
		"labels": df['itemtype'].to_list()
	}]

	print(json.dumps(chart))

except Exception as e:
	print(str(e))

finally:
	cur.close()
	conn.close()
