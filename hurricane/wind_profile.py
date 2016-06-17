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


df = pd.read_csv('hurricane_test_v2.dat.gz', compression = 'gzip', sep=',' , error_bad_lines=False)

print df

datetime = df.iloc[:,5:6]
forecast_time = df.iloc[:,2:3]
pn = 1010
pc = df.iloc[:,9:10]
Vmax = df.iloc[:,8:9]
Rmax = df.iloc[:,19:20]
roci = df.iloc[:,18:19]
Vtrans = df.iloc[:,23:24]
lat = df.iloc[:,6:7].replace(to_replace='N', value='', regex=True).replace(to_replace='S', value='', regex=True)

pc_float = (np.float64(pc.values[5]).item())
Vmax_float = (np.float64(Vmax.values[5]).item())
Rmax_float = (np.float64(Rmax.values[5]).item())
roci_float = (np.float64(roci.values[5]).item())
Vtrans_float = (np.float64(Vtrans.values[5]).item())
theta_float = (np.float64(lat.values[5]).item())


f = 2*7.292*math.sin(theta_float)
rho = 1.15
e = 2.7182

#Dir = []
#phi = []

B = int((((Vmax_float**2)*rho*e)*(1/(pn - pc_float))))

A = int(Rmax_float)**int(B)

print Rmax_float
print B


wind_speed = []

print range(1,int(roci_float))

for i in range(1,int(roci_float)):
	Vg = (mp.sqrt(((A*B*(pn - int(pc_float)))*mp.exp(int(-A)/int(i)**int(B))/((((int(rho)*int(i)**int(B) + int(i)**2*int(f)**2)/4))))) - (int(i)* int(f)/2))
	print Vg
