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
select * from allstormspts_4326 where name = '{}' and season = {}""".format(hurricane_name, hurricane_name, hurricane_year)

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

bash_syntax = ' ' 

for data in range_feat:
	bash_syntax += ' ' + str(data)

bash_deconstruct = 'for i in ' + bash_syntax + ' ' + str(range_feat_strp_v2) + ' ; do pgsql2shp -f {}_$i.shp hamlethurricane "select * from hurricane_{} where id = $i"; done'.format(hurricane_name, hurricane_name)
 
call(bash_deconstruct, shell = True) 

print bash_deconstruct

bash_reconstruct = 'for i in ' + bash_syntax + ' ' + str(range_feat_strp_v2) + ' ; do ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlethurricane password=password" {}_$i.shp -t_srs EPSG:4326 -overwrite; done'.format(hurricane_name)

print bash_reconstruct

call(bash_reconstruct, shell = True)

conn.close()
