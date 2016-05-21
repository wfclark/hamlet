import sys
import os
import urllib2
import datetime
import time
import psycopg2
import pandas
from subprocess import call, Popen

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

print "Connecting to database..."

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

print "Connected!\n"

dataframe_cur = conn.cursor()

dataframe_cur.execute("""Select * from hurricane_katrina""")

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

buffer_cur = conn.cursor()

for key in range(1,len(dataframe) + 1):
	
	sql = """create or replace view vw_rmw_{} as
	select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), (select distinct atc_roci from katrina_{})*1069),4326)::geometry(polygon, 4326) as geom from katrina_{} limit 1;""".format(key, key, key)

	print sql 

	buffer_cur.execute(sql)
	conn.commit()

update_cur = conn.cursor() 

for key in range(1, len(dataframe) + 1):
	
 	sql = """update table hurricane_katrina 
 	set set a.geom = b.geom from vw_rmw_{}
 	where a.iso_time = b.iso_time""".format(key) 

 	print sql 

 	update_cur.execute(sql)
 	conn.commit()
conn.close()
