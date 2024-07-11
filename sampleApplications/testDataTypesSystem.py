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
import nzpy
#params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.153;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# print(params)
# engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator, echo=True) #working
print (engine)

meta = MetaData()
# For lowercase & uppercase system
TEST = Table(
   '_v_object_data', meta,
Column('objid',nz.OID),
Column('owner',nz.NAME),
Column('createdate',nz.ABSTIME),
Column('description',nz.TEXT),
)
'''
# Fails with lowercase system
TEST = Table(
   '_v_object_data', meta,
Column('OBJID',nz.OID),
Column('OWNER',nz.NAME),
Column('CREATEDATE',nz.ABSTIME),
Column('DESCRIPTION',nz.TEXT),
)
'''

#meta.drop_all(engine);
#meta.create_all(engine);
conn = engine.connect()
#data = conn.execute(select([TEST.c.OWNER,TEST.c.CREATEDATE]).limit(10)).fetchall()
data = conn.execute(TEST.select().limit(10)).fetchall()
for row in data:
     print (row)
