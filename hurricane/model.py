
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


#last_1hr_shp = './latest/last_1_hours.shp'
#last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_1hr_qpe -overwrite'.format(last_1hr_shp)
#print last_hr_shp2pgsql
#call(last_hr_shp2pgsql, shell = True)

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

print "Connecting to database..."

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

print "Connected!\n"


dataframe_cur = conn.cursor()

dataframe_cur.execute("""Select * from hurricane_irene""")

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

print data
print dataframe

conn.commit()

num_feat = len(data)

for i in range(len(data))
	os.system('pgsql2shp -f {} -u postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_1hr_qpe -overwrite ').

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
