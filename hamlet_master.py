#Dataset descriptions

#NSSL: Waiting for response from Carrie.Langston@noaa.gov

	# 209	6	0	PrecipFlag	2-min	flag	-1	n/a	-3	Surface Precipitation Type (Convective, Stratiform, Tropical, Hail, Snow) 

	# 209	6	1	PrecipRate	2-min	mm/hr	-1	n/a	-3	Radar Precipitation Rate

	# 209	6	2	RadarOnlyQPE01H	2-min	mm	-1	n/a	-3	Radar precipitation accumulation 1-hour

	# 209	6	9	GaugeCorrQPE01H	60-min	mm	-1	n/a	-3	Local gauge bias corrected radar precipitation accumulation 1-hour	

	# 209	6	23	MountainMapperQPE01H

	# 60-min	mm	-1	n/a	-3	Mountain Mapper precipitation accumulation 1-hour

#NWS AND WPC(GFS MOS, QPF observations)

#Quantitative Precipitation Forecast in point format on a 2.5 kilometer grid
	 #http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/
		#dateset names : last_1_hours.tar.gz



#Snow accumulation percentiles for 24 and 48 hour periods in raster format on a 2.5 km grid. 
	 #Source : http://www.wpc.ncep.noaa.gov/pwpf/wwd_percentiles.php?fpd=24&ptype=snow&ftype=percentiles
	 	#Data:12 hr
	 	#		endpoint :ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_12hr/
	 	#		dataset name: 2.5kmpicez_2016020912f012.grb 
 		#	  48 hr 
 		#		endpoint :ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_48hr/2.5kmpicez48_2016021000f048.grb
	 	#		dataset name:  2.5kmpicez48_2016021000f048.grb
	 	#**These data change per time and date, which need to be corrected for dynamically
#Dataset descriptions

#NSSL: Waiting for response from Carrie.Langston@noaa.gov

	# 209	6	0	PrecipFlag	2-min	flag	-1	n/a	-3	Surface Precipitation Type (Convective, Stratiform, Tropical, Hail, Snow) 

	# 209	6	1	PrecipRate	2-min	mm/hr	-1	n/a	-3	Radar Precipitation Rate

	# 209	6	2	RadarOnlyQPE01H	2-min	mm	-1	n/a	-3	Radar precipitation accumulation 1-hour

	# 209	6	9	GaugeCorrQPE01H	60-min	mm	-1	n/a	-3	Local gauge bias corrected radar precipitation accumulation 1-hour	

	# 209	6	23	MountainMapperQPE01H

	# 60-min	mm	-1	n/a	-3	Mountain Mapper precipitation accumulation 1-hour

#NWS AND WPC(GFS MOS, QPF observations)

#Quantitative Precipitation Forecast in point format on a 2.5 kilometer grid
	 #http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/
		#dateset names : last_1_hours.tar.gz



#Snow accumulation percentiles for 24 and 48 hour periods in raster format on a 2.5 km grid. 
	 #Source : http://www.wpc.ncep.noaa.gov/pwpf/wwd_percentiles.php?fpd=24&ptype=snow&ftype=percentiles
	 	#Data:12 hr
	 	#		endpoint :ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_12hr/
	 	#		dataset name: 2.5kmpicez_2016020912f012.grb 
 		#	  48 hr 
 		#		endpoint :ftp://ftp.hpc.ncep.noaa.gov/pwpf/conus_2.5km/2.5kmpwpf_48hr/2.5kmpicez48_2016021000f048.grb
	 	#		dataset name:  2.5kmpicez48_2016021000f048.grb
	 	#**These data change per time and date, which need to be corrected for dynamically

import urllib2
import datetime
import time
import psycopg2
from subprocess import call, Popen
import os

# #----------PRECIP ACCUMULATION FOR EVERY HOUR FORMATED FOR GEOSPATIAL PROCESSES--------------#
# #pull the last hours worth of precip data 
os.system("wget http://www.srh.noaa.gov/ridge2/Precip/qpehourlyshape/latest/last_1_hours.tar.gz")

# #Convert into tar
os.system("mv last_1_hours.tar.gz last_1_hours.tar")

# #Expand tar file
os.system("tar xvf last_1_hours.tar")

# #Name of extracted GIS layer for the last hour of precip. This shapefile will be used as the indepedent variable to intersect on to the depedent variables of the street features and impervious surfaces polygon. This of course is being done here in Buncombe County North Carolina.These intersections will determine where r traffic might be due to the current driving conditions. As well as a notification system that tells the user which roads not to take. These kinds of geospatial processes can be applied to a variety of industrial problems such as the money lost due to buisness interruptions. For example this tool could be used to predict the amount of weather induced delays that a cargo ship could run into when trying to arrive to port. If there is a delay in the cargo, the supply, then there would be a shortage to the demand. This weather induced shortage of supplies could potentially cause a change in corporations stock on the market.

last_hr_shp = '/var/www/html/hamlet/latest/last_1_hours.shp'

last_hr_shp2pgsql = 'ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlet password=password"'+ ' '+ last_hr_shp  + ' ' + "-t_srs EPSG:4326 -nln last_hr_prcp"

print last_hr_shp2pgsql

call(last_hr_shp2pgsql, shell = True)



#-----Begin geospatial analysis with PostGIS
#After these data have been vectorized and projected to scale we begin the intersection of environmental variables to infrastructure. Infrastructure data consist of impervious services from the national land cover database as well as the street lines from the Buncombe County GIS department. 

conn_string = "dbname='hamlet' user=postgres port='5432' host='127.0.0.1' password='password'"

please_wait = 'Please wait.....'
# # print the connection string we will use to connect
print "Connecting to database\n	->%s" % (please_wait)

# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)
 
print "Connected!\n"

# conn.cursor will return a cursor object, you can use this cursor to perform queries
ham_cur = conn.cursor()

#creating views that show where the roads are potentially flooded or exposed to icy conditions

ham_cur.execute("""CREATE table prcp_rd AS
 				 SELECT p.wkb_geometry FROM last_hr_prcp as p
 				 JOIN buncombe_streets_4326 as streets 
 				 on ST_Contains(p.wkb_geometry,streets.geom);""")


# ham_cur.execute("""CREATE table icy_roads AS
#  				 SELECT snw.wkb_geometry FROM snow_accum AS
#  				 snw JOIN buncombe_streets_4326 as streets 
#  				 on ST_intersects(snw.wkb_geometry, streets.geom);""")

conn.commit()
ham_cur.close()


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


