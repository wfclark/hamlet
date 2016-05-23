import sys
import os
import datetime
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

hurricane_name = 'ARTHUR'

dataframe_cur = conn.cursor()

dataframe_sql = """Select * from hurricane_{}""".format(hurricane_name)

dataframe_cur.execute(dataframe_sql)

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

range_feat =  range(len(dataframe))

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

print range_feat_strp_v2

bash_syntax = ' ' 

for data in range_feat:
	bash_syntax += ' ' + str(data)

drop_if_geo_sql = """drop table if exists hurricane_{}_geo""".format(hurricane_name)

drop_if_geo_cur = conn.cursor()

drop_if_geo_cur.execute(drop_if_geo_sql)

creation_cur = conn.cursor()

creation_sql = """create table hurricane_{}_geo as 
select * from hurricane_{}""".format(hurricane_name,hurricane_name)

creation_cur.execute(creation_sql)


conn.commit()

drop_cur = conn.cursor()

drop_sql = """alter table hurricane_{}_geo 
drop column geom""".format(hurricane_name)

drop_cur.execute(drop_sql) 

conn.commit()

add_cur = conn.cursor()

add_sql = """
alter table hurricane_{}_geo
add column geom geometry(polygon, 4326), 
add column impact numeric""".format(hurricane_name)

add_cur.execute(add_sql)

conn.commit()

buffer_cur = conn.cursor() 

for key in range(1,len(dataframe)-1):
	
	sql = """create or replace view vw_rmw_{} as
	select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
	(select distinct atc_rmw from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
	from {}_{} limit 1;""".format(key, hurricane_name, key, hurricane_name, key)

	print sql 

	buffer_cur.execute(sql)
	conn.commit()


intersect_cur = conn.cursor()

for key in range(1,len(dataframe)-1):
	
	sql = """create or replace view vw_impact_{} as
	select b.iso_time, b.ogc_fid, sum(parval) as impact, a.geom::geometry(polygon, 4326) as geom 
	from dare_4326 as a join vw_rmw_{} as b on st_intersects(a.geom,b.geom)
	group by b.iso_time, b.ogc_fid, a.geom;""".format(key, key)

	print sql 

	intersect_cur.execute(sql)
	conn.commit()


update_cur = conn.cursor() 

for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set geom = b.geom 
 	from vw_rmw_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	print sql 

 	update_cur.execute(sql)
 	conn.commit()
 
for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set impact = b.impact
 	from vw_impact_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	print sql 

 	update_cur.execute(sql)
 	conn.commit()

drop_dismembered_cur = conn.cursor() 

for key in range(1, len(dataframe)-1):
	
 	sql = """drop table if exists {}_{} cascade""".format(hurricane_name, key) 

 	print sql 

 	drop_dismembered_cur.execute(sql)
 	conn.commit() 



conn.close()
