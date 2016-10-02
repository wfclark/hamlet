import sys
import os
import datetime
import psycopg2
import pandas
from subprocess import call, Popen

print "buffering and intersecting the features..."

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

hurricane_name = 'ARTHUR'

dataframe_cur = conn.cursor()

dataframe_sql = """Select * from hurricane_{}""".format(hurricane_name)

dataframe_cur.execute(dataframe_sql)

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

range_feat =  range(len(dataframe)-1)

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

drop_if_sql = """drop table if exists hurricane_{}_geo""".format(hurricane_name)

drop_if_cur = conn.cursor()

drop_if_cur.execute(drop_if_sql)

conn.commit()

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

add_sql = """alter table hurricane_{}_geo
		 	add column geom geometry(polygon, 4326), 
			add column impact numeric,
			add column county character varying(50),
			add column parcel_count numeric""".format(hurricane_name)

add_cur.execute(add_sql)

conn.commit()

buffer_cur = conn.cursor() 

for key in range(1,len(dataframe)-1):
	
	sql = """create or replace view vw_rmw_{} as
	select iso_time, ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), 
	(select distinct atc_rmw from {}_{})*1069),4326)::geometry(polygon, 4326) as geom 
	from {}_{} limit 1;""".format(key, hurricane_name, key, hurricane_name, key)

	buffer_cur.execute(sql)
	conn.commit()

intersect_cur = conn.cursor()

for key in range(1,len(dataframe)-1):
	
	sql = """create or replace view vw_hurricane_impact_{} as
	select b.iso_time, b.ogc_fid, sum(parval) as impact, a.cntyname as county, count(a.gid) as parcel_count, b.geom::geometry(polygon, 4326) as geom 
	from dare_4326 as a join vw_rmw_{} as b on st_intersects(a.geom,b.geom)
	group by b.iso_time, b.ogc_fid, b.geom, county;""".format(key, key)

	intersect_cur.execute(sql)
	conn.commit()


update_cur = conn.cursor() 

for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set geom = b.geom
 	from vw_rmw_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	update_cur.execute(sql)
 	conn.commit()

update_cur = conn.cursor() 

for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set geom = b.geom
 	from vw_rmw_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	update_cur.execute(sql)
 	conn.commit()

numbers_cur = conn.cursor() 
 
for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set impact = b.impact
 	from vw_hurricane_impact_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	numbers_cur.execute(sql)
 	conn.commit()

name_cur = conn.cursor()

for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set county = b.county
 	from vw_hurricane_impact_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	name_cur.execute(sql)
 	conn.commit()
 
count_cur = conn.cursor() 

for key in range(1, len(dataframe)-1):
	
 	sql = """update hurricane_{}_geo as a
 	set parcel_count = b.parcel_count
 	from vw_hurricane_impact_{} as b
 	where a.iso_time = b.iso_time""".format(hurricane_name, key) 

 	count_cur.execute(sql)
 	conn.commit()

##############################################Start of geoprocess on parcel geometric members###########################################

drop_if_sql = """drop table if exists hurricane_{}_parcels, exposed_parcels""".format(hurricane_name)

drop_if_cur = conn.cursor()

drop_if_cur.execute(drop_if_sql)

creation_cur = conn.cursor()

creation_sql = """create table hurricane_{}_parcels as 
				  select  * from dare_4326""".format(hurricane_name,hurricane_name)

creation_cur.execute(creation_sql)

conn.commit()

add_cur = conn.cursor()

add_sql = """alter table hurricane_{}_parcels
			 add column andrew_impact character varying(50),
			 add column iso_time character varying (19)
			 """.format(hurricane_name)

add_cur.execute(add_sql)

conn.commit()

buffer_cur = conn.cursor() 


intersect_cur = conn.cursor()

for key in range(1,len(dataframe)-1):
	
	sql = """create or replace view vw_parcels_impact_{} as
	select a.nparno, b.iso_time, b.ogc_fid, a.geom as geom 
	from dare_4326 as a 
	inner join vw_rmw_{} as b 
	on st_intersects(b.geom,a.geom)
	group by a.nparno, b.iso_time, b.ogc_fid, a.geom;""".format(key, key)

	intersect_cur.execute(sql)
	conn.commit()

update_cur = conn.cursor() 

for key in range(1, len(dataframe)-1):
	
  	sql = """update hurricane_{}_parcels as a
  	set iso_time = b.iso_time 
  	from vw_parcels_impact_{} as b
 	where a.nparno = b.nparno""".format(hurricane_name, key) 

 	update_cur.execute(sql)
 	conn.commit()