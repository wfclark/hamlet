
import sys
import os
import datetime
import psycopg2
import pandas
from subprocess import call, Popen

print "running exposure algorithim.."

conn_string = "dbname='hamlethurricane' user=postgres port='5432' host='127.0.0.1' password='password'"

try:
	conn = psycopg2.connect(conn_string)
except Exception as e:
	print str(e)
	sys.exit()

hurricane_name = 'ARTHUR'

dataframe_cur = conn.cursor()

dataframe_sql = """Select * from hurricane_{}""".format(hurricane_name)

dataframe_cur.execute(dataframe_sql)

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

range_feat =  range(len(dataframe)-1)

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

drop_if_cur = conn.cursor()

drop_if_sql = """drop table if exists exposed_parcels, summary_statistics cascade"""

drop_if_cur.execute(drop_if_sql)

conn.commit()

exposed_cur = conn.cursor()
	
exposed_sql = """create table exposed_parcels as 
  				select  * from hurricane_{}_parcels where iso_time is not null""".format(hurricane_name, hurricane_name) 

exposed_cur.execute(exposed_sql)

summary_cur = conn.cursor()

conn.commit()

summary_sql = """create table summary_statistics as 
			  select sum(parval) as dollars_exposed, 
			  count(gid), iso_time, cntyname from exposed_parcels
			  group by iso_time, cntyname; """ 

summary_cur.execute(summary_sql)

summary_stats_cur = conn.cursor()

summary_stats_sql = """Select * from summary_statistics"""

summary_stats_cur.execute(summary_stats_sql)

data = summary_stats_cur.fetchall()

colnames = [desc[0] for desc in summary_stats_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

print 'The summary statistics are' 
print dataframe
