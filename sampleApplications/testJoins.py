import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
#!/usr/bin/env python3
import pg8000
import urllib
from datetime import datetime
from sqlalchemy import select
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime, ForeignKey
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR

params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)

engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
print (engine)

meta = MetaData()
conn = engine.connect()
students = Table(
   'STUDENTS', meta, 
   Column('id', Integer, primary_key = True), 
   Column('name', VARCHAR(25)), 
   Column('lastname', VARCHAR(25)), 
)

addresses = Table(
   'ADDRESSES', meta, 
   Column('id', Integer, primary_key = True), 
   Column('st_id', Integer, ForeignKey('STUDENTS.id')), 
   Column('postal_add', VARCHAR(25)), 
   Column('email_add', VARCHAR(25))
)
test = Table(
 'TST', meta,
  Column('id', Integer, primary_key = True)
)
meta.drop_all(engine)
meta.create_all(engine)
ins = students.insert()
ins = test.insert()

conn.execute(students.insert(), [
   {'id':13,'name':'Ravi', 'lastname':'Kapoor'},
   {'id':14,'name':'Rajiv', 'lastname' : 'Khanna'},
   {'id':15,'name':'Komal','lastname' : 'Bhandari'},
   {'id':16,'name':'Abdul','lastname' : 'Sattar'},
   {'id':17,'name':'Priya','lastname' : 'Rajhans'},
])
conn.execute(addresses.insert(), [
   {'id':1,'st_id':13, 'postal_add':'Shivajinagar Pune', 'email_add':'ravi@gmail.com'},
   {'id':2,'st_id':14, 'postal_add':'ChurchGate Mumbai', 'email_add':'kapoor@gmail.com'},
   {'id':3,'st_id':15, 'postal_add':'MG Road Bangaluru', 'email_add':'as@yahoo.com'},
   {'id':4,'st_id':13, 'postal_add':'Jubilee Hills Hyderabad', 'email_add':'komal@gmail.com'},
   {'id':5,'st_id':17, 'postal_add':'Cannought Place new Delhi', 'email_add':'admin@khanna.com'},
])

s = select([students])
result = conn.execute(s)
print ("students")
for row in result:
    print (row)

print ("addresses")
s = select([addresses]).distinct()
result = conn.execute(s)
for row in result:
    print (row)

from sqlalchemy import join
from sqlalchemy.sql import select
j = students.join(addresses, students.c.id == addresses.c.st_id)
stmt = select([students]).select_from(j)
result = conn.execute(stmt)
for row in result:
    print (row)

stmt = select([addresses]).select_from(j)
result = conn.execute(stmt)
for row in result:
    print (row)


stmt = students.update().\
values({
   students.c.name:'xyz',
   addresses.c.email_add:'abc@xyz.com'
}).\
where(students.c.id == addresses.c.id)
#conn.execute(stmt) #Error: tries to set email_add in student

stmt = students.update().\
   values(name = 'xyz').\
   where(students.c.id == addresses.c.id)
conn.execute(stmt) #update_from_clause 

result = conn.execute(students.select())
for res in result:
    print (res)


stmt = students.delete().\
   where(students.c.id == addresses.c.st_id).\
   where(addresses.c.email_add.startswith('admin%'))
conn.execute(stmt) #delete_extra_from_clause

result = conn.execute(students.select())
for res in result:
    print (res)

from sqlalchemy import and_
stmt = select([students]).where(and_(students.c.name == 'Ravi', students.c.id <3))
conn.execute(stmt)

#UNION
from sqlalchemy import union, union_all, except_, intersect
u = union(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)
result.fetchall()

u = union_all(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)

#EXCEPT
u = except_(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)

#INTERSECT
u = intersect(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)

