import os
import sys
import urllib
import nzpy
from sqlalchemy import create_engine, MetaData, Table, Column, select
import nzalchemy as nz

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
test = Table(  
   'TEST', meta, 
   Column('id', nz.INTEGER), 
   Column('name', nz.VARCHAR(20) ), 
   Column('gender', nz.CHAR),
)
meta.create_all(engine)

conn = engine.connect()

conn.execute(test.insert(),[
                             {'id':2,'name':'xyz','gender':'F'},
                             {'id':3,'name':'abc','gender':'M'},
                            ]
             )

print ("After Insert")
s = select(test)
result = conn.execute(s)
for row in result:
    print (row)

updt = test.update().where(test.c.id == '2').values(name='changed1')
conn.execute(updt)
s = select(test)
result = conn.execute(s)
for row in result:
    print (row)

delt = test.delete().where(test.c.name == 'changed1')
conn.execute(delt)
s = select(test)
result = conn.execute(s)
for row in result:
    print (row)


