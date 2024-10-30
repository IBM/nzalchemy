import os
import sys
import nzpy
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime, ForeignKey
from sqlalchemy import select
from sqlalchemy import join
from sqlalchemy import and_
from sqlalchemy import union, union_all, except_, intersect
from sqlalchemy.sql import select
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR


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

#insert into students and addresses
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

s = select(students)
result = conn.execute(s)
print ("students")
for row in result:
    print (row)

print ("addresses")
s = select(addresses).distinct()
result = conn.execute(s)
for row in result:
    print (row)

print("->1")
j = students.join(addresses, students.c.id == addresses.c.st_id)
stmt = select(students).select_from(j)
result = conn.execute(stmt)
for row in result:
    print (row)

print("->2")
stmt = select(addresses).select_from(j)
result = conn.execute(stmt)
for row in result:
    print (row)

print("->3")
stmt = students.update().\
   values(name = 'xyz').\
   where(students.c.id == addresses.c.id)
conn.execute(stmt) #update_from_clause 
result = conn.execute(students.select())
for res in result:
    print (res)

print("->4")
stmt = students.delete().\
   where(students.c.id == addresses.c.st_id).\
   where(addresses.c.email_add.startswith('admin%'))
conn.execute(stmt)
result = conn.execute(students.select())
for res in result:
    print (res)

print("->5")
stmt = select(students).where(and_(students.c.name == 'Ravi', students.c.id <3))
conn.execute(stmt)

u = union(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)
for res in result:
    print (res)

print("->6")
u = union_all(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)
for res in result:
    print (res)

print("->7")
u = except_(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))
result = conn.execute(u)
for res in result:
    print (res)
