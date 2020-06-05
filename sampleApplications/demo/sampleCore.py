import pyodbc
import urllib
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime, text, asc, desc, alias, text, between, func
from datetime import datetime
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR
from sqlalchemy import select, insert

metadata = MetaData()

params = urllib.parse.quote_plus("DRIVER=NetezzaSQL;SERVER=longpassword1.fyre.ibm.com;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
engine = create_engine("nz+pyodbc:///?odbc_connect=%s" % params, echo=True)

meta = MetaData()
employee = Table(
   'employee', meta,
   Column('id', Integer),
   Column('name', VARCHAR(20) ),
   Column('gender', CHAR),
)
meta.create_all(engine)

print(employee.columns)         # return a list of columns
print(employee.c)               # same as posts.columns
print(employee.foreign_keys)    # returns a set containing foreign keys on the table
print(employee.primary_key)     # returns the primary key of the table
print(employee.metadata)        # get the MetaData object from the table

#conn for insert and select
conn = engine.connect()

# different ways to insert
ins = employee.insert().values(id='21',name='jack', gender='M')
result = conn.execute(ins)
print(result)
print(ins.compile().params)

ins = insert(employee).values(id='25',name='hijack', gender='M')
result = conn.execute(ins)
print(result)
print(ins.compile().params)

#multiple insert
conn.execute(employee.insert(), [
        {'id':19,'name':'crack', 'gender':'F'},
        {'id':13,'name':'track', 'gender':'M'}
])

#func
result = conn.execute(select([func.max(employee.c.id)]))
print (result.fetchone())

#select
s = select([employee])  
result = conn.execute(s)
for row in result:
    print (row)
    
#another way to select
s = employee.select().where(employee.c.id>20)
result = conn.execute(s)
for row in result:
    print (row)

# like statement
s = select([employee]).where(employee.c.name.like("ja%"))
result = conn.execute(s)
for row in result:
    print (row)

#select using limit
s = select([employee]).limit(2)  
result = conn.execute(s)
for row in result:
    print (row)

# order by    
s = select([employee]).order_by(desc(employee.c.id)) 
print(conn.execute(s).fetchall())
   
#alias   
st = employee.alias("a")
s = select([st]).where(st.c.id > 2)
print(conn.execute(s).fetchall())
    
#using text firing query    
t = text("SELECT * FROM employee limit 2")
print(conn.execute(t).fetchall())

#using text and binding params
s = text("select id, gender from employee where id between :x and :y")
print(conn.execute(s, x = 10, y = 22).fetchall())

# using between
stmt = select([employee]).where(between(employee.c.id,20,40))
print (conn.execute(stmt).fetchall())

#update
updt = employee.update().where(employee.c.id == '13').values(name='changed')
result = conn.execute(updt)
s = select([employee])
result = conn.execute(s)
for row in result:
    print (row)
    
#Delete Row/s
delt = employee.delete().where(employee.c.name == 'changed')
conn.execute(delt)
s = select([employee])
result = conn.execute(s)
for row in result:
    print (row)    

#drop table
meta.drop_all(engine)
