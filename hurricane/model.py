import sys
import os
import urllib2
import datetime
import time
import psycopg2
import pandas
from subprocess import call, Popen

# pull the 6 hr forecast track forecast from NHC  
#os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_1_hours.tar.gz -O last_1_hours.tar.gz")
#os.system("mv last_1_hours.tar.gz last_1_hours.tar")

#os.system("tar xvf last_1_hours.tar")


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

alter_cur = conn.cursor()

#alter_cur.execute("""alter tabke hyrrucabe_katrian add id serial""")
#alter_cur.execute("""alter table hurricane_katrina add id serial""")

print dataframe

conn.commit()

num_feat = len(data)

range_feat =  range(num_feat)

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

test = ' ' 

for data in range_feat:
	test += ' ' + str(data)

#clear up file run on clean runs  

print test

bash_rm='for i in 1 ' + test + ' ' + str(num_feat) + ' ; do sudo rm katrina_$i.* ; done'

#call(bash_rm, shell = True)

print range_feat_strp_v2

bash_deconstruct = 'for i in 1 ' + test + ' ' + str(num_feat) + ' ; do pgsql2shp -f katrina_$i.shp hamlethurricane "select * from hurricane_katrina where id = $i"; done'

call(bash_deconstruct, shell = True) 

print bash_deconstruct

bash_reconstruct = 'for i in 1 ' + test + ' ' + str(num_feat) + ' ; do ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlethurricane password=password" katrina_$i.shp -t_srs EPSG:4326; done'

print bash_reconstruct

call(bash_reconstruct, shell = True)


for key in range(len(dataframe)):
	
	sql = """create or replace view vw_rmw_{} as
	select ogc_fid, st_transform(st_buffer(st_transform(wkb_geometry,32612), (select distinct atc_roci from irene_56)*1069),4326)
	as wkb_geometry as geom from irene_{} limit 1;""".format(key)

	print sql 

	cur.execute(sql)
	conn.commit()

for key in range(len(dataframe)):
	
	sql = """update table hurricane_katrina 
	set set a.geom = b.geom from vw_rmw_{}
	where a.iso_time = b.iso_time""".format(key) 

	print sql 

	cur.execute(sql)
	conn.commit()
	



#os.system(bash)

#drop_cur.close()

# hurricane_cur = conn.cursor() 

# hurricane_cur.execute("""
# create table roads_flooded_bunco as 
# select
# a.gid,
# street_nam,
# sum(b.globvalue),
# a.geom
# from conterlines_poly as a
# inner join last_1hr_qpe as b 
# on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
# group by a.gid, a.street_nam, a.geom;""")


# conn.commit()
