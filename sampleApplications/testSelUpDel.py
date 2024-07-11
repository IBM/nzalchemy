import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
#!/usr/bin/env python3
#import pdb
import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

import pg8000
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime
from datetime import datetime
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR
from sqlalchemy import select
import urllib
import nzpy


#params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.153;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# print(params)
#engine = create_engine("postgres+pg8000://postgres@localhost:5432/db1", echo=True) #working
# engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator, echo=True) #working
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

#Insert Method1
#ins = TEST3.insert().values(id='1',name='jack1', gender='M')
#result = conn.execute(ins)

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


