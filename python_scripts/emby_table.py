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
	SELECT to_char("datecreated", 'DD HH12:MI AM') AS "date", "itemname", "devicename" 
	FROM playbackactivity ORDER BY "datecreated" DESC LIMIT 15;
	'''
	cur.execute(fetch_query)
	rows = cur.fetchall()
	df = pd.DataFrame(rows, columns=['date', 'itemname', 'devicename'])
	print(df.to_json(orient='records'))

except Exception as e:
	print(str(e))

finally:
	cur.close()
	conn.close()
