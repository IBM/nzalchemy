import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, select, desc,Sequence
from sqlalchemy.types import BIGINT
from sqlalchemy.types import BOOLEAN
from sqlalchemy.types import CHAR
from sqlalchemy.types import DATE
from sqlalchemy.types import FLOAT
from sqlalchemy.types import INTEGER
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import REAL
from sqlalchemy.types import SMALLINT
from sqlalchemy.types import TEXT
from sqlalchemy.types import VARCHAR

import urllib
import datetime
import nzalchemy as nz

##Engine Creation
params = urllib.parse.quote_plus("DRIVER=NetezzaSQL;SERVER=longpassword1.fyre.ibm.com;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)


from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
class Customers(Base):
   __tablename__ = 'CUSTOMERS'
   
   id = Column(nz.SMALLINT, Sequence('USR_ID_SEQ3'), primary_key = True)
   name = Column(VARCHAR(30))
   address = Column(nz.NVARCHAR(30))
   email = Column(nz.NCHAR(30))

Customers.__table__.drop(engine, checkfirst=True)
Customers.__table__.create(engine, checkfirst=True)

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind = engine)
session = Session()

#INSERT
c1 = Customers(name = 'Ravi Kumar', address = 'Station Road Nanded', email = 'ravi@gmail.com') 
session.add(c1)
session.commit()

session.add_all([
   Customers(id = 2, name = 'Komal Pande', address = 'Koti, Hyderabad', email = 'komal@gmail.com'), 
   Customers(id = 3, name = 'Rajender Nath', address = 'Sector 40, Gurgaon', email = 'nath@gmail.com'), 
   Customers(id = 4, name = 'S.M.Krishna', address = 'Budhwar Peth, Pune', email = 'smk@gmail.com')]
)
session.commit()

# Count
# SELECT count(*) AS count_1 FROM (SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address",
# "CUSTOMERS".email AS "CUSTOMERS_email" FROM "CUSTOMERS") AS anon_1
res = session.query(Customers).count()
print (res)


# select all 
result = session.query(Customers).all()
for row in result:
   print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)


# limit  1  
row = session.query(Customers).first() 
print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)


# filter and update
# UPDATE "CUSTOMERS" SET name=(? || "CUSTOMERS".name) WHERE "CUSTOMERS".id != ?
session.query(Customers).filter(Customers.id != 2).update({Customers.name:"Mr."+Customers.name}, synchronize_session = False)


# SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address", "CUSTOMERS".email AS "CUSTOMERS_email"
# FROM "CUSTOMERS" WHERE "CUSTOMERS".id > ?
result = session.query(Customers).filter(Customers.id>2)
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)


# SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address", "CUSTOMERS".email AS "CUSTOMERS_email"
# FROM "CUSTOMERS" WHERE "CUSTOMERS".name LIKE ?
result = session.query(Customers).filter(Customers.name.like('Ra%'))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)


# SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address", "CUSTOMERS".email AS "CUSTOMERS_email"
# FROM "CUSTOMERS" WHERE "CUSTOMERS".id IN (?, ?)
result = session.query(Customers).filter(Customers.id.in_([1,3]))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)


# SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address", "CUSTOMERS".email AS "CUSTOMERS_email"
# FROM "CUSTOMERS" WHERE "CUSTOMERS".id > ? OR "CUSTOMERS".name LIKE ?
from sqlalchemy import or_
result = session.query(Customers).filter(or_(Customers.id>2, Customers.name.like('Ra%')))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)


# SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address", "CUSTOMERS".email AS "CUSTOMERS_email"
# FROM "CUSTOMERS" WHERE "CUSTOMERS".id = ?
session.query(Customers).filter(Customers.id == 3).scalar()


# SELECT "CUSTOMERS".id AS "CUSTOMERS_id", "CUSTOMERS".name AS "CUSTOMERS_name", "CUSTOMERS".address AS "CUSTOMERS_address", "CUSTOMERS".email AS "CUSTOMERS_email"
# FROM "CUSTOMERS" WHERE id<3
from sqlalchemy import text
for cust in session.query(Customers).filter(text("id<3")):
   print(cust.name)


#column_descriptions 
print (session.query(Customers).column_descriptions)


