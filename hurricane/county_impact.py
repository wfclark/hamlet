import sys
import os
import datetime
import psycopg2
import pandas as pd
import numpy as np
from subprocess import call, Popen

df = pd.read_csv('katrina_test_3.dat.gz', compression = 'gzip', sep=',', error_bad_lines=False, header=None, skiprows = 5)
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

for length in range(1,len(df), 1):
	b_slice.append(((((Vmax.values[length]).item())**2)*rho*e)/(pn - (np.float64(pc.values[length]).item()*.514) * 100))

# A = int(Rmax_float)**int(B)
hurricane_df = pd.DataFrame({'forecast_hr' : forecast_hr_slice,'datetime': datetime_slice, 'rmax': rmax_slice, 'vmax': vmax_slice, 'roci': roci_slice, 'roci_miles': roci_slice_miles, 'lat': lat_slice, 'lon' : lon_slice, 'pc' :pc_slice, 'b':b_slice})

hurricane_df_clean = hurricane_df[hurricane_df.rmax != 0]

datetime = []

for length in range(1,len(hurricane_df_clean),4):
	datetime.append(int(hurricane_df_clean['datetime'].values[length].item()))

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

impact_cur = conn.cursor()

for key in range(1,(len(datetime)-1)):
	
	sql = """create or replace view vw_county_impact_{} as
	select a.ctfips, avg(b."Windspeed")
	from county as a 
	inner join katrina_pnt_{} as b 
	on st_intersects(b.geom,a.geom)
	group by a.ctfips""".format(str(datetime[key]), key)

	impact_cur.execute(sql)
	conn.commit()
