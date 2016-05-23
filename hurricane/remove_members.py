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

hurricane_name = 'ARTHUR'

hurricane_year = '2014'

pull_cur = conn.cursor() 
 
drop_exists_sql = """drop table if exists hurricane_{} cascade""".format(hurricane_name)

pull_cur.execute(drop_exists_sql)

pull_sql = """create table hurricane_{} as
select * from allpoints_lines_4326 where name = '{}' and season = {}""".format(hurricane_name, hurricane_name, hurricane_year)

pull_cur.execute(pull_sql) 

alter_og_sql = """alter table hurricane_{} add column id serial""".format(hurricane_name)

pull_cur.execute(alter_og_sql)

conn.commit() 

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

print range_feat_strp_v2

for key in range(1,len(dataframe)-1):
	
	remove_members = 'sudo rm {}_{}.*'.format(hurricane_name, key)
	print remove_members
	os.system(remove_members)
