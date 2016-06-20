import pandas
import sys
import os
import urllib2
import datetime
import time
import psycopg2
import math
from subprocess import call, Popen
import pandas as pd
import gzip
import csv
import numpy as np
import decimal
from mpmath import mp
import matplotlib.pyplot as plt
import matplotlib 


df = pd.read_csv('hurricane_test_v2.dat.gz', compression = 'gzip', sep=',' , error_bad_lines=False)

df

print df

datetime = df.iloc[:,5:6]
print datetime
forecast_time = df.iloc[:,2:3]

print forecast_time
pn = 101000
pc = df.fillna(0).iloc[:,9:10]
print pc
Vmax = df.fillna(0).iloc[:,8:9]
print Vmax

Rmax = df.fillna(0).iloc[:,19:20]
print Rmax
roci = df.fillna(0).iloc[:,18:19] 
Vtrans = df.fillna(0).iloc[:,23:24]
lat = df.fillna(0).iloc[:,6:7].replace(to_replace='N', value='', regex=True).replace(to_replace='S', value='', regex=True)
print lat

pc_float = (np.float64(pc.values[5]).item()) * 100 
Vmax_float = (np.float64(Vmax.values[5]).item()) / .514
Rmax_float = (np.float64(Rmax.values[5]).item()) / .539
roci_float = (np.float64(roci.values[5]).item()) / .514
Vtrans_float = (np.float64(Vtrans.values[5]).item()) / .514
theta_float = (np.float64(lat.values[5]).item())


f = 2*(7.292*10**-5)*math.sin(math.radians(theta_float))
rho = 1.15
e = 2.7182

#Dir = []
#phi = []

B = int((((Vmax_float**2)*rho*e)/(pn - pc_float)))

A = int(Rmax_float)**int(B)

print Rmax_float
print B

wind_speed = []

radius_roci = []

print range(1,int(roci_float))


for i in range(1,int(roci_float),1):
	Vg = (mp.sqrt((((A*B*(pn - int(pc_float)))*mp.exp(int(-A)/(int(i)**int(B))))/(((int(rho)*int(i)**int(B) + ((int(i)**2*int(f)**2)/4))))))) - (((int(i)* int(f))/2))
	wind_speed.append(float(Vg))
	radius_roci.append(i)
	print Vg


df_windspeed = pd.DataFrame(wind_speed)
df_radius = pd.DataFrame(radius_roci)

plot_dataframe = pd.DataFrame({'Radius (km)' : radius_roci, 'Windspeed' : wind_speed})

#print plot_dataframe
#print datetime
#print forecast_time

plt.figure()

wind_speed_plot = plot_dataframe.plot(x ='Radius (km)', y='Windspeed', label = 'Computed Wind Speed (m/s)'); plt.legend(loc ='best')

wind_speed_plot.set_ylabel('Wind Speed (m/s)')
wind_speed_plot.set_xlabel('Radius (km)')

plt.show()


import matplotlib.pyplot as plt
