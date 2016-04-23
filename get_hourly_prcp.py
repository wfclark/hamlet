import sys
import os
import urllib2
import datetime
import time
import psycopg2
from subprocess import call, Popen

# pull the last hours worth of precip data 
os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_1_hours.tar.gz -O last_1_hours.tar.gz")
os.system("mv last_1_hours.tar.gz last_1_hours.tar")
os.system("tar xvf last_1_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_2_hours.tar.gz -O last_2_hours.tar.gz")
os.system("mv last_2_hours.tar.gz last_2_hours.tar")
os.system("tar xvf last_2_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_3_hours.tar.gz -O last_3_hours.tar.gz")
os.system("mv last_3_hours.tar.gz last_3_hours.tar")
os.system("tar xvf last_3_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_6_hours.tar.gz -O last_6_hours.tar.gz")
os.system("mv last_6_hours.tar.gz last_6_hours.tar")
os.system("tar xvf last_6_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_12_hours.tar.gz -O last_12_hours.tar.gz")
os.system("mv last_12_hours.tar.gz last_12_hours.tar")
os.system("tar xvf last_12_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_24_hours.tar.gz -O last_24_hours.tar.gz")
os.system("mv last_24_hours.tar.gz last_24_hours.tar")
os.system("tar xvf last_24_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpfshp/latest/latest_rqpf_f006.tar.gz.tar.gz -O latest_rqpf_f006.tar.tar.gz")
os.system("mv last_12_hours.tar.gz last_12_hours.tar")
os.system("tar xvf last_12_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/latest_rqpf_f012.tar.gz -O latest_rqpf_f012.tar.gz")
os.system("mv last_24_hours.tar.gz last_24_hours.tar")
os.system("tar xvf last_24_hours.tar")

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/latest_rqpf_f024.tar.gz -O latest_rqpf_f024.tar.gz")
os.system("mv last_24_hours.tar.gz last_24_hours.tar")
os.system("tar xvf last_24_hours.tar")

last_1hr_shp = './latest/last_1_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_1hr_qpe -overwrite'.format(last_1hr_shp)
print last_hr_shp2pgsql
call(last_hr_shp2pgsql, shell = True)

last_2hr_shp = './latest/last_2_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_2hr_qpe -overwrite'.format(last_2hr_shp)
print last_hr_shp2pgsql
call(last_hr_shp2pgsql, shell = True)

last_3hr_shp = './latest/last_3_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_3hr_qpe -overwrite'.format(last_3hr_shp)
print last_hr_shp2pgsql
call(last_hr_shp2pgsql, shell = True)

last_6hr_shp = './latest/last_6_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_3hr_qpe -overwrite'.format(last_6hr_shp)
print last_hr_shp2pgsql
call(last_hr_shp2pgsql, shell = True)

last_12hr_shp = './latest/last_12_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_12hr_qpe -overwrite'.format(last_12hr_shp)
print last_hr_shp2pgsql
call(last_hr_shp2pgsql, shell = True)

last_24hr_shp = './latest/last_24_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_24hr_qpe -overwrite'.format(last_24hr_shp)
print last_hr_shp2pgsql
call(last_hr_shp2pgsql, shell = True)
# Begin geospatial analysis with PostGIS

conn_string = "dbname='hamlet' user=postgres port='5432' host='127.0.0.1' password='password'"

print "Connecting to database..."

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

print "Connected!\n"

drop_cur = conn.cursor()

#creating views that show where the roads are potentially flooded or exposed to icy conditions

drop_cur.execute("""drop table if exists roads_flooded_bunco cascade;""")

drop_cur.execute("""drop table if exists roads_flooded_se_heavy cascade;""")

drop_cur.execute("""drop table if exists roads_flooded_se_moderate cascade;""")

drop_cur.execute("""drop table if exists roads_flooded_se_light cascade;""")

drop_cur.execute("""drop table if exists roads_flooded_se_drizzle cascade;""")

conn.commit()
drop_cur.close()

flooded_cur = conn.cursor() 

flooded_cur.execute("""
create table roads_flooded_bunco as 
select
a.gid,
street_nam,
sum(b.globvalue),
a.geom
from conterlines_poly as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
group by a.gid, a.street_nam, a.geom;""")

flooded_cur.execute("""create table roads_flooded_se_heavy as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= 1
group by a.gid, a.geom;""")

flooded_cur.execute("""create table roads_flooded_se_moderate as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= .5
group by a.gid, a.geom;
""")

flooded_cur.execute("""create table roads_flooded_se_light as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= .25
group by a.gid, a.geom;""")

flooded_cur.execute("""create table roads_flooded_se_drizzle as 
select
gid
street_nam,
sum(b.globvalue),
a.geom
from se_road_polys as a
inner join last_hr_prcp as b 
on st_dwithin(a.geom::geometry(MULTIpolygon, 4326), b.wkb_geometry::geometry(point, 4326), 0.025)
where b.globvalue >= .1 and b.globvalue <= .25
group by a.gid, a.geom;""")



