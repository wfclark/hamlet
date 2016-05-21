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

hurricane_name = 'SANDY'

hurricane_year = '2012'

pull_cur = conn.cursor() 

pull_sql = """'create table hurricane_{} as
select * from allstormspts_4326 where name = {} and season = {}""".format(hurricane_name, hurricane_name, hurricane_year)

pull_cur.execute(pull_sql) 

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

bash_deconstruct = 'for i in 1 ' + bash_syntax + ' ' + str(range_feat_strp_v2) + ' ; do pgsql2shp -f hugo_$i.shp hamlethurricane "select * from hurricane_{} where id = $i"; done'.format(hurricane_name)
 
call(bash_deconstruct, shell = True) 

print bash_deconstruct

bash_reconstruct = 'for i in ' + bash_syntax + ' ' + str(range_feat_strp_v2) + ' ; do ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlethurricane password=password" {}_$i.shp -t_srs EPSG:4326; done'.format(hurricane_name)

print bash_reconstruct

call(bash_reconstruct, shell = True)

creation_cur = conn.cursor()

creation_sql = """create table hurricane_{}_geo as 
select * from hurricane_katrina """.format(hurricane_name)

creation_cur.execute(creation_cur)

conn.commit()

drop_cur = conn.cursor()

drop_sql = """alter table hurricane_{}_geo 
drop column geom""".format(hurricane_name)

drop_cur.execute(drop_sql) 

conn.commit()

add_cur = conn.cursor()

add_sql = """
alter table hurricane_{}_geo
add column geom geometry(polygon, 4326)""".format(hurricane_name)

conn.commit()

conn.close()
