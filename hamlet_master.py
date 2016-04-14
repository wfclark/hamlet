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

last_hr_shp = './latest/last_1_hours.shp'
last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password" {} -t_srs EPSG:4326 -nln last_hr_prcp -overwrite'.format(last_hr_shp)
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

ham_cur = conn.cursor()

#creating views that show where the roads are potentially flooded or exposed to icy conditions

ham_cur.execute("""create or replace view last_hr_heavy as select * from last_hr_prcp where globvalue >= .25;""")

ham_cur.execute("""create or replace view select * from roads, last_hr_heavy where st_dwithin(roads.geom, last_hr_heavy.wkb_geometry, 2500)""")

ham_cur.execute("""SELECT
  a.gid AS roads,
  b.id AS last_hr_heavy,
  CASE 
     WHEN ST_Within(a.geom,b.wkb_geometry) 
     THEN a.geom
     ELSE ST_Multi(ST_Intersection(a.geom,b.wkb_geometry)) 
  END AS geom
FROM roads as a ,last_hr_heavy as b 
where st_dwithin(a.geom, b.wkb_geometry, 2500);
""")
	 
conn.commit()
ham_cur.close()


# ICE STUFF
# hamzur.execute("""CREATE table icy_roads AS
#  				 SELECT snw.wkb_geometry FROM snow_accum AS
#  				 snw JOIN buncombe_streets_4326 as streets 
#  				 on ST_intersects(snw.wkb_geometry, streets.geom);""")

#----Snow accumulation grb formating and importing for geospatial processes---------------#

#Note these accumlations can be oberserved over a 24, 12, 48, 72 hour time frame pull
## snow accumulation data from NOAA server in grb format

# os.system("wget ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_48hr/2.5kmpicez48_2016032712f048.grb")

#

# #Translate grb it into Geotiff and project to EPSG:4326
# os.system("gdalwarp -t_srs EPSG:4326 2.5kmpicez48_2016021312f072.grb snow_accum_4326.tif")

# #Convert and clip the raster into a vector format for a county
# os.system("gdalwarp -cutline buncombe_county.shp snow_accum_4326.tif bc_snow_accum_4326.tif")


# #Take all of the values and make them there own polygons
# os.system("gdal_calc.py -A snow_accum_4326.tif --A_band=1 --outfile=snow_accum_4326_poly.tif --co COMPRESS=LZW --co BIGTIFF=YES --calc='1*(A>=257)' --NoDataValue=0")

# #take the recalculated raster abd define polygons
# os.system('gdal_polygonize.py -8 bc_snow_accum_4326.tif -f "ESRI Shapefile" bc_snow_accum_4326.shp')

# #Snow accumulation shp vector into PostGIS using gdal

# snow_accum_shp = 'bc_snow_accum_4326.shp'


# snow_accum_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=ubuntu dbname=hamlet password=BILLYbob123"'+ ' '+ snow_accum_shp  + ' ' + '-t_srs EPSG:4326 -nln snow_accum'

# print snow_accum_shp2pgsql

# call(snow_accum_shp2pgsql, shell = True)


