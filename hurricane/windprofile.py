import psycopg2
import math
from subprocess import call, Popen
import pandas as pd
import numpy as np
from mpmath import mp
import matplotlib.pyplot as plt
import matplotlib 
from geopy import Point
from geopy.distance import distance, VincentyDistance
from sqlalchemy import create_engine
import itertools as it


df = pd.read_csv('katrina_test_3.dat.gz', compression = 'gzip', sep=',', error_bad_lines=False, header=None, skiprows = 5)

number_of_profile = 17

#define constants
# f = 2*(7.292*10**-5)*math.sin(np.deg2rad(lat_float))
rho = 1.15
e = 2.7182
w = 0
zeta = 0
n = .85
X1 = 288.5
X2 = 75
A = 0.1
pn = 101200
Vtrans = 5
# print x.loc[:, (df != 0)]

forecast_time = df.iloc[:,5:6]

date = df.iloc[:,2:3]

pc = df.fillna(0).iloc[:,9:10]

Vmax = df.fillna(0).iloc[:,8:9]

Rmax = df.fillna(0).iloc[:,19:20]

roci = df.fillna(0).iloc[:,18:19] 

lat = df.fillna(0).iloc[:,6:7].replace(to_replace='N', value='', 
	regex=True).replace(to_replace='S', value='', regex=True)


lon = df.fillna(0).iloc[:,7:8].replace(to_replace='W', value='', 
	regex=True).replace(to_replace='E', value='', regex=True)


transition = df.fillna(0).iloc[:,26:27] 

forecast_hr_slice = []
datetime_slice = []
pc_slice = [] 
vmax_slice = []
rmax_slice = []
roci_slice = []
roci_slice_miles = []
lat_slice = []
lon_slice = []
b_slice = []
transition_slice = []

for length in range(1,len(df), 1):
	datetime_slice.append((np.float64(date.values[length].item())))

for length in range(1,len(df), 1):
	forecast_hr_slice.append((np.float64(forecast_time.values[length].item())))

for length in range(1,len(df), 1):
	pc_slice.append((np.float64(pc.values[length]).item()) * 100)

for length in range(1,len(df), 1):
	vmax_slice.append((np.float64(Vmax.values[length]).item()) * .514)

for length in range(1,len(df), 1):
	rmax_slice.append((np.float64(Rmax.values[length]).item()) * 1.852)

for length in range(1,len(df), 1):
	roci_slice.append((np.float64(roci.values[length]).item()) * 1.852)

for length in range(1,len(df), 1):
	roci_slice_miles.append((np.float64(roci.values[length]).item()) * 1.15) 

for length in range(1, len(df), 1):
	lat_slice.append((np.float64(lat.values[length]).item()) / 10)

#longitude for the united states 
# #if else statement for west vs east and north vs south
for length in range(1,len(df), 1):
	lon_slice.append((-1 * (np.float64(lon.values[length]).item()) / 10))




for length in range(1,len(df),1):
	transition_slice.append((transition.values[length].item()))

# A = int(Rmax_float)**int(B)
hurricane_df = pd.DataFrame({'forecast_hr' : forecast_hr_slice,'datetime': datetime_slice, 'rmax': rmax_slice, 'vmax': vmax_slice, 'roci': roci_slice, 'roci_miles': roci_slice_miles, 'lat': lat_slice, 'lon' : lon_slice, 'pc' :pc_slice, 'vtrans' : transition_slice})


hurricane_df_clean = hurricane_df[hurricane_df.rmax != 0]

datetime_clean = []
rmax_clean = [] 
vmax_clean = []
roci_clean = []
roci_clean_miles = []
lat_clean = []
lon_clean = []
roci_clean = []
b_clean = []
vtrans_clean = []


for length in range(1,len(hurricane_df_clean),4):
	datetime_clean.append(int(hurricane_df_clean['datetime'].values[length].item()))

for length in range(1,len(hurricane_df_clean),4):
	rmax_clean.append(hurricane_df_clean['rmax'].values[length].item())

for length in range(1,len(hurricane_df_clean),4):
	vmax_clean.append(hurricane_df_clean['vmax'].values[length].item())

for length in range(1,len(hurricane_df_clean),4):
	roci_clean.append(hurricane_df_clean['roci'].values[length].item())

for length in range(1,len(hurricane_df_clean),4):
	roci_clean_miles.append(hurricane_df_clean['roci_miles'].values[length].item())

for length in range(1,len(hurricane_df_clean),4):
	lat_clean.append(hurricane_df_clean['lat'].values[length].item())

for length in range(1,len(hurricane_df_clean),4):
	lon_clean.append(hurricane_df_clean['lon'].values[length].item())

for length in range(1,len(hurricane_df_clean),4): 
	vtrans_clean.append(int(hurricane_df_clean['vtrans'].values[length]) *.514)

for length in range(1,len(hurricane_df_clean),4):
	b_clean.append(((((Vmax.values[length]).item())**2)*rho*e)/(pn - (np.float64(hurricane_df_clean['pc'].values[length]).item())))

windspeed = []
roci_new = []
lat_new = []
lon_new = []
bearing_angle = []
distance_from = []
latitude_halfi = []
longitude_halfi = []
latitude_halfii = []
longitude_halfii = []
motion_vector = []
datetime_new = []
motion_vector_halfi= [] 
motion_vector_halfii = []

def windprofile(vmax, rmax, b, roci_, datetime):
	for r in xrange(1, int(roci_)):
			windspeed.append(float(vmax*((((rmax * .001)/(r * .001)**1.5)*mp.exp(1-(((rmax * .001)/(r * .001)**1.5)))))))
			roci_new.append(r)
			datetime_new.append(str(datetime))

def windvector_halfi(roci_, vtrans):
	for r in xrange(1,len(roci_)):
		for angle in range(0, 180):
			motion_vector_halfi.append(float(windspeed[r]+(float((vtrans*abs(math.sin(np.deg2rad(angle))))))))

def windvector_halfii(roci_, vtrans):
 	for r in xrange(1,len(roci_)):
		for angle in range(180, 361):
			motion_vector_halfii.append(float((windspeed[r] - float((2*vtrans*abs(math.sin(np.deg2rad(angle))))))))
		


def hurricane_halfi(roci_, lat, lon):
	for r in xrange(1,len(roci_)):
		for bearing in range(0, 180):
			bearing_angle.append(bearing)
			distance_from.append(r)
			destination = VincentyDistance(kilometers=r).destination(Point(lat, lon), bearing)
			lat2,lon2 = destination.latitude, destination.longitude
			latitude_halfi.append(lat2)
			longitude_halfi.append(lon2)


def hurricane_halfii(roci_, lat, lon):
	for r in xrange(1,len(roci_)):
		for bearing in range(180, 361):
			bearing_angle.append(bearing)
			distance_from.append(r)
			destination = VincentyDistance(kilometers=r).destination(Point(lat, lon), bearing)
			lat2,lon2 = destination.latitude, destination.longitude
			latitude_halfii.append(lat2)
			longitude_halfii.append(lon2)



windprofile(vmax_clean[number_of_profile], rmax_clean[number_of_profile], b_clean[number_of_profile], 
roci_clean[number_of_profile], datetime_clean[number_of_profile])

windvector_halfi(roci_new, vtrans_clean[number_of_profile]) 

hurricane_halfi(roci_new, lat_clean[number_of_profile], lon_clean[number_of_profile])

windvector_halfii(roci_new, vtrans_clean[number_of_profile]) 

hurricane_halfii(roci_new, lat_clean[number_of_profile], lon_clean[number_of_profile])

halfi_df = pd.DataFrame({'lat' : latitude_halfi, 'lon' : longitude_halfi, 'translation' : motion_vector_halfi})


halfii_df = pd.DataFrame({'lat' : latitude_halfii, 'lon' : longitude_halfii, 'translation' : motion_vector_halfii })


conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

engine = create_engine('postgresql://postgres:password@127.0.0.1/hamlethurricane')


halfi_tb = 'katrina_halfi_{}'.format(number_of_profile)

halfii_tb = 'katrina_halfii_{}'.format(number_of_profile)

halfi_df.to_sql(halfi_tb, con = engine)
halfii_df.to_sql(halfii_tb, con = engine)

conn.commit()

profile_cur = conn.cursor()

halfi_sql = """create or replace view katrina_halfi_pnt_{} as select *, ST_SetSRID(ST_MakePoint("lon", "lat"), 4326) as geom from katrina_halfi_{}""".format(number_of_profile, number_of_profile)

halfii_sql = """create or replace view katrina_halfii_pnt_{} as select *, ST_SetSRID(ST_MakePoint("lon", "lat"), 4326) as geom from katrina_halfii_{}""".format(number_of_profile, number_of_profile)

profile_cur.execute(halfi_sql)
profile_cur.execute(halfii_sql)

conn.commit()

merge_sql = """create or replace view katrina_merge_{} as 
			   select * from katrina_halfi_pnt_{}
			   union all 
			   select * from katrina_halfii_pnt_{}""".format(number_of_profile, number_of_profile, number_of_profile)


profile_cur.execute(merge_sql) 

conn.commit()

conn.close()

# #------------------------------------------------------#

# print len(roci_new), len(motion_vector_test)

# plot_dataframe = pd.DataFrame({'Radius (km)' : roci_new, 'Windspeed' : motion_vector_test})
# plot_dataframe = plot_dataframe.astype(float)

# plt.figure()

# wind_speed_plot = plot_dataframe.plot(x ='Radius (km)', y='Windspeed', label = 'Vg')

# plt.xlabel('Radius (km)')
# plt.ylabel('Radial Windspeed (m/s)')
# plt.title('0 Degree Vg')
# # plt.plot(x='Radius', y = 'Windspeed')

# plt.show()

# import matplotlib.pyplot as plt


# print "the values that went into this profile were" + ' ' + str(pc_float) + ' ' + str(Vmax_float) + ' ' + str(Rmax_float)+ ' '  + str(roci_float) + ' ' + str(Vtrans_float) +  ' ' + str(theta_float)