import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
import logging
import pg8000
import urllib
import nzpy

from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR
from sqlalchemy import select


def creator():
    return nzpy.connect(user="admin", password="password",host='myhost', port=5480, database="db", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator, echo=True)
print (engine)

meta = MetaData()
TEST3 = Table(  
   'TEST3', meta, 
   Column('id', Integer), 
   Column('name', VARCHAR(20) ), 
   Column('gender', CHAR),
)
meta.create_all(engine)

#conn for insert and select
conn = engine.connect()

#Insert Method2 Multiple Inserts
conn.execute(TEST3.insert(),[
                             {'id':2,'name':'xyz','gender':'F'},
                             {'id':3,'name':'abc','gender':'M'},
                            ]
             )

#Select
print ("After Insert")
s = select(TEST3)
result = conn.execute(s)
for row in result:
    print (row)

#Update
updt = TEST3.update().where(TEST3.c.id == '2').values(name='changed1')
conn.execute(updt)
s = select(TEST3)
result = conn.execute(s)
for row in result:
    print (row)

#Delete Row/s
delt = TEST3.delete().where(TEST3.c.name == 'changed1')
conn.execute(delt)
s = select(TEST3)
result = conn.execute(s)
for row in result:
    print (row)


