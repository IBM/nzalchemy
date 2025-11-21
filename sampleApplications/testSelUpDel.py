import os
import sys
import nzpy
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR
from sqlalchemy import select

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
TEST3 = Table(  
   'TEST3', meta, 
   Column('id', Integer), 
   Column('name', VARCHAR(20) ), 
   Column('gender', CHAR),
)
meta.create_all(engine)

conn = engine.connect()

conn.execute(TEST3.insert(),[
                             {'id':2,'name':'xyz','gender':'F'},
                             {'id':3,'name':'abc','gender':'M'},
                            ]
             )

print ("After Insert")
s = select(TEST3)
result = conn.execute(s)
for row in result:
    print (row)

updt = TEST3.update().where(TEST3.c.id == '2').values(name='changed1')
conn.execute(updt)
s = select(TEST3)
result = conn.execute(s)
for row in result:
    print (row)

delt = TEST3.delete().where(TEST3.c.name == 'changed1')
conn.execute(delt)
s = select(TEST3)
result = conn.execute(s)
for row in result:
    print (row)
