import sys
import os
import datetime
import psycopg2
import pandas
from subprocess import call, Popen

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

print "Connecting to database..."

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

print "Connected!\n"


dataframe_cur = conn.cursor()

dataframe_cur.execute("""Select * from hurricane_hugo""")

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

range_feat =  range(len(dataframe))

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

print range_feat_strp_v2

bash_syntax = ' ' 

for data in range_feat:
	bash_syntax += ' ' + str(data)

clear up file run on clean runs  

bash_rm='for i in 1 ' + bash_syntax + ' ' + str(range_feat_strp_v2) + ' ; do sudo rm katrina_$i.* ; done'

call(bash_rm, shell = True)

