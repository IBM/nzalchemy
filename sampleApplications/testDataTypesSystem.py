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

def creator():
    return nzpy.connect(user="admin", password="password",host='host', port=5480, database="db", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
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


conn = engine.connect()
data = conn.execute(TEST.select().limit(10)).fetchall()
for row in data:
     print (row)
