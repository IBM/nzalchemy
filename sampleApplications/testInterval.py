import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
from sqlalchemy import create_engine, MetaData, Table, Column
'''
import pg8000
from sqlalchemy.dialects import postgresql as ps
engine = create_engine("postgres+pg8000://postgres@localhost:5432/db1", echo=True) #working
'''

import nzalchemy as nz
import urllib
params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
print (engine)

meta = MetaData()
TEST = Table(
   'TESTDT_INTERVAL', meta,
#Column("NZ_TIMETZ",nz.TIMETZ),
#Column("NZ_TIME",nz.TIME),
#Column("NZ_TIME_TZ",nz.TIME.with.time.zone)), #time with time zone 
Column("NZ_INTERVAL", nz.INTERVAL), #Error while retriving even empty #ODBC SQL type 110 is not yet supported

)
meta.drop_all(engine)
meta.create_all(engine)

conn = engine.connect()
conn.execute(TEST.insert().values(
#NZ_TIMETZ= '12:57:42 AEST',  #Error while inserting with AEST
#NZ_TIMETZ= '12:57:42+05:30',  #Expects in +/- hh:mm format
#NZ_TIME= '12:19:23',
NZ_INTERVAL= '1y4mon3d23:34:57.232', #Doesn't allow ='1y4mon3d23h34m67s234565ms'

))
result = conn.execute(TEST.select())
for row in result:
    print (row)
