import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
#!/usr/bin/env python3
from sqlalchemy import create_engine, MetaData, Table, Column, select
import nzalchemy as nz
import urllib

params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
print (engine)

meta = MetaData()
test = Table(  
   'TEST', meta, 
   Column('id', nz.INTEGER), 
   Column('name', nz.VARCHAR(20) ), 
   Column('gender', nz.CHAR),
)
meta.create_all(engine)

#conn for insert and select
conn = engine.connect()

#Insert Method1
#ins = test.insert().values(id='1',name='jack1', gender='M')
#result = conn.execute(ins)

#Insert Method2 Multiple Inserts
conn.execute(test.insert(),[
                             {'id':2,'name':'xyz','gender':'F'},
                             {'id':3,'name':'abc','gender':'M'},
                            ]
             )

#Select
print ("After Insert")
s = select([test])
result = conn.execute(s)
for row in result:
    print (row)

#Update
updt = test.update().where(test.c.id == '2').values(name='changed1')
conn.execute(updt)
s = select([test])
result = conn.execute(s)
for row in result:
    print (row)

#Delete Row/s
delt = test.delete().where(test.c.name == 'changed1')
conn.execute(delt)
s = select([test])
result = conn.execute(s)
for row in result:
    print (row)


