#Datasets that I am using: 

#NSSL:

	# 209	6	0	PrecipFlag	2-min	flag	-1	n/a	-3	Surface Precipitation Type (Convective, Stratiform, Tropical, Hail, Snow) 

	# 209	6	1	PrecipRate	2-min	mm/hr	-1	n/a	-3	Radar Precipitation Rate

	# 209	6	2	RadarOnlyQPE01H	2-min	mm	-1	n/a	-3	Radar precipitation accumulation 1-hour

	# 209	6	9	GaugeCorrQPE01H	60-min	mm	-1	n/a	-3	Local gauge bias corrected radar precipitation accumulation 1-hour	

	# 209	6	23	MountainMapperQPE01H

	# 60-min	mm	-1	n/a	-3	Mountain Mapper precipitation accumulation 1-hour

#NWS (GFS MOS, WPC)
#Type Precip
	 #http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/
		#D=Names : last_1_hours.tar.gz
#Type Snow accumulation 
	 #Source : http://www.wpc.ncep.noaa.gov/pwpf/wwd_percentiles.php?fpd=24&ptype=snow&ftype=percentiles
	 	#Data : ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_12hr/
	 	#D = Names : 2.5kmpicez_2016020912f012.grb 

import urllib2
import zipfile 
import datetime
import time
import psycopg2
from subprocess import call, Popen
import os


conn_string = "dbname='geopdf' user='postgres' password='9502f8c6-ca21-44d7-bf02-2d5a7dc471fa'"


please_wait = 'Please wait.....'
# print the connection string we will use to connect
print "Connecting to database\n	->%s" % (please_wait)
 
# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)
 
print "Connected!\n"
#Pull hourly precip data in point format and import into postgis

os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_1_hours.tar.gz")

#Convert into usable format
os.system("mv last_1_hours.tar.gz last_1_hours.tar")

os.system("tar xvf last_1_hours.tar")

shp_file = 'last_1_hours.shp'

shp2pgsql = "ogr2ogr -f PostgreSQL PG:'user=postgres dbname=hamlet'"+ 'password =9502f8c6-ca21-44d7-bf02-2d5a7dc471fa' +' ' + shp_file 

print shp2pgsql

call(shp2pgsql, shell = True)

##pull snow accumulation data in grb format then translat it into tiff----------

os.system("wget ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_12hr/2.5kmpicez_2016020912f012.grb")

#grb into a more useable format gtiff" 
os.system("gdalwarp -t_srs EPSG:4326 2.5kmpicez_2016020912f012.grb 4326_grib.tif")

##Snow accumulation into sql formt for PostGIS processes
os.system("raster2pgsql -s 4326 -I -C -M *.tif -F -t 100x100 public.snow_accum > snow_accum.sql")

##Convert and clip the raster into a vector format for a county
os.system("gdalwarp -cutline centerlines.shp 4326_grib.tif bc_cut_4326.tif")

#shp2pgsql = "ogr2ogr -f PostgreSQL PG:'user=postgres dbname=hamlet'"+ ' ' + shp_file 
os.system("gdal_polygonize.py -8  bc_cut_4326.tif -f ESRI Shapefile bc_cut_4326.shp")
