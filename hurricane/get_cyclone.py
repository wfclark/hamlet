import sys
import os
import datetime
import psycopg2
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

dataframe_cur.execute("""Select * from hurricane_katrina""")

data = dataframe_cur.fetchall()

colnames = [desc[0] for desc in dataframe_cur.description]

dataframe = pandas.DataFrame(data)

dataframe.columns = colnames

conn.commit()

range_feat =  range(dataframe)

range_feat_strp = str(range_feat).strip('[]')

range_feat_strp_v2 = range_feat_strp.split(',')

bash_syntax = ' ' 

for data in range_feat:
	bash_syntax += ' ' + str(data)

#clear up file run on clean runs  

bash_rm='for i in 1 ' + bash_syntax + ' ' + str(num_feat) + ' ; do sudo rm katrina_$i.* ; done'

call(bash_rm, shell = True)

#breaks cyclone data into simple features 

bash_deconstruct = 'for i in 1 ' + bash_syntax + ' ' + str(num_feat) + ' ; do pgsql2shp -f katrina_$i.shp hamlethurricane "select * from hurricane_katrina where id = $i"; done'

call(bash_deconstruct, shell = True) 

print bash_deconstruct

bash_reconstruct = 'for i in 1 ' + bash_syntax + ' ' + str(num_feat) + ' ; do ogr2ogr -f "PostgreSQL" PG:"user=postgres dbname=hamlethurricane password=password" katrina_$i.shp -t_srs EPSG:4326; done'

print bash_reconstruct

call(bash_reconstruct, shell = True)

creation_cur = conn.cursor()

creation_cur.execute("""create table hurricane_katrina_geo as 
select * from hurricane_katrina """)

conn.commit()

alter_cur = conn.cursor()

alter_cur.execute("""alter table hurricane_katrina_geo 
drop column geom""") 

conn.commit()
