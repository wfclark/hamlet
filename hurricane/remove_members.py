import sys
import os
import datetime
import psycopg2
import pandas
from subprocess import call, Popen

print "removing temporary files..."

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

os.system('exit')

hurricane_name = 'ARTHUR'

dataframe_cur = conn.cursor()

dataframe_sql = """Select * from hurricane_{}""".format(hurricane_name)

dataframe_cur.execute(dataframe_sql)

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

range_feat =  range(len(dataframe))

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

for key in range(1,len(dataframe)):
	
	remove_members = 'sudo rm {}_{}.*'.format(hurricane_name, key)
	os.system(remove_members)

print "finish"
