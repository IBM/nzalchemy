import os
import sys
import nzpy
import urllib
import nzalchemy as nz
from sqlalchemy import create_engine, MetaData, Table, Column

print ("\n--------- " + sys.argv[0] + " ---------\n")

host = os.getenv("MY_HOST")
user = os.getenv("MY_USER")
password = os.getenv("MY_PASSWORD")
db = os.getenv("MY_DB")
port = os.getenv("MY_PORT")

def creator():
    return nzpy.connect(user=f"{user}", password=f"{password}",host=f"{host}", port=int(port), database=f"{db}", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator)
print (engine)

meta = MetaData()
TEST = Table(
   '_v_object_data', meta,
Column('objid',nz.OID),
Column('owner',nz.NAME),
Column('createdate',nz.ABSTIME),
Column('description',nz.TEXT),
)

conn = engine.connect()
data = conn.execute(TEST.select().limit(10)).fetchall()
for row in data:
     print (row)
