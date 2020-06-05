import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, select, desc, ForeignKey
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
#from sqlalchemy import select
import urllib
import datetime
import nzalchemy as nz

##Engine Creation
params = urllib.parse.quote_plus("DRIVER=NetezzaSQL;SERVER=longpassword1.fyre.ibm.com;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)


from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy.orm import relationship

class Customer(Base):
   __tablename__ = 'CUSTOMER'

   id = Column(Integer, primary_key = True)
   name = Column(VARCHAR(30))
   address = Column(VARCHAR(30))
   email = Column(VARCHAR(30))

class Invoice(Base):
   __tablename__ = 'INVOICE'
   
   id = Column(Integer, primary_key = True)
   custid = Column(Integer, ForeignKey('CUSTOMER.id'))
   invno = Column(Integer)
   amount = Column(Integer)
   customer = relationship("Customer", back_populates = "INVOICE")

Customer.INVOICE = relationship("Invoice", order_by = Invoice.id, back_populates = "customer")

Customer.__table__.drop(engine, checkfirst=True)
Invoice.__table__.drop(engine, checkfirst=True)
Base.metadata.create_all(engine)

c1 = Customer(id=2, name = "Gopal Krishna", address = "Bank Street Hydarebad", email = "gk@gmail.com")
c1.INVOICE = [Invoice(id=3, invno = 10, amount = 15000), Invoice(id=4, invno = 14, amount = 3850)]

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind = engine)
session = Session()
session.add(c1)
session.commit()

c2 = [
   Customer(
      id = 3,
      name = "Govind Pant", 
      address = "Gulmandi Aurangabad",
      email = "gpant@gmail.com",
      INVOICE = [Invoice(id=5,invno = 3, amount = 10000), 
      Invoice(id=6,invno = 4, amount = 5000)]
   )
]
rows = [
   Customer(
      id = 4,
      name = "Govind Kala", 
      address = "Gulmandi Aurangabad", 
      email = "kala@gmail.com", 
      INVOICE = [Invoice(id=7,invno = 7, amount = 12000), Invoice(id=8,invno = 8, amount = 18500)]),

   Customer(
      id = 5, 
      name = "Abdul Rahman", 
      address = "Rohtak", 
      email = "abdulr@gmail.com",
      INVOICE = [Invoice(id=9,invno = 9, amount = 15000), 
      Invoice(id=10,invno = 11, amount = 6000)
   ])
]

session.add_all(c2)
session.commit()
session.add_all(rows)
session.commit()


# SELECT "CUSTOMER".id AS "CUSTOMER_id", "CUSTOMER".name AS "CUSTOMER_name", "CUSTOMER".address AS "CUSTOMER_address", "CUSTOMER".email AS "CUSTOMER_email", 
# "INVOICE".id AS "INVOICE_id", "INVOICE".custid AS "INVOICE_custid", "INVOICE".invno AS "INVOICE_invno", "INVOICE".amount AS "INVOICE_amount"
# FROM "CUSTOMER", "INVOICE" WHERE "CUSTOMER".id = "INVOICE".custid
for c, i in session.query(Customer, Invoice).filter(Customer.id == Invoice.custid).all():
   print ("ID: {} Name: {} Invoice No: {} Amount: {}".format(c.id,c.name, i.invno, i.amount))


# SELECT "CUSTOMER".id AS "CUSTOMER_id", "CUSTOMER".name AS "CUSTOMER_name", "CUSTOMER".address AS "CUSTOMER_address", "CUSTOMER".email AS "CUSTOMER_email"
# FROM "CUSTOMER" JOIN "INVOICE" ON "CUSTOMER".id = "INVOICE".custid WHERE "INVOICE".amount = ?
result = session.query(Customer).join(Invoice).filter(Invoice.amount == 8500)
for row in result:
   for inv in row.INVOICE:
      print (row.id, row.name, inv.invno, inv.amount)


# SELECT "CUSTOMER".id AS "CUSTOMER_id", "CUSTOMER".name AS "CUSTOMER_name", "CUSTOMER".address AS "CUSTOMER_address", "CUSTOMER".email AS "CUSTOMER_email", 
# anon_1.invoice_count AS anon_1_invoice_count FROM "CUSTOMER" LEFT OUTER JOIN (SELECT "INVOICE".custid AS custid, count(?) AS invoice_count
# FROM "INVOICE" GROUP BY "INVOICE".custid) AS anon_1 ON "CUSTOMER".id = anon_1.custid ORDER BY "CUSTOMER".id 
from sqlalchemy.sql import func
stmt = session.query(
   Invoice.custid, func.count('*').label('invoice_count')
).group_by(Invoice.custid).subquery()

for u, count in session.query(Customer, stmt.c.invoice_count).outerjoin(stmt, Customer.id == stmt.c.custid).order_by(Customer.id):
   print(u.name, count)


# SELECT "CUSTOMER".id AS "CUSTOMER_id", "CUSTOMER".name AS "CUSTOMER_name", "CUSTOMER".address AS "CUSTOMER_address", "CUSTOMER".email AS "CUSTOMER_email"
# FROM "CUSTOMER" WHERE "CUSTOMER".name = ?

# SELECT "INVOICE".id AS "INVOICE_id", "INVOICE".custid AS "INVOICE_custid", "INVOICE".invno AS "INVOICE_invno", "INVOICE".amount AS "INVOICE_amount", 
# anon_1."CUSTOMER_id" AS "anon_1_CUSTOMER_id" FROM (SELECT "CUSTOMER".id AS "CUSTOMER_id" FROM "CUSTOMER"
# WHERE "CUSTOMER".name = ?) AS anon_1 JOIN "INVOICE" ON anon_1."CUSTOMER_id" = "INVOICE".custid ORDER BY "INVOICE".id
from sqlalchemy.orm import subqueryload
c1 = session.query(Customer).options(subqueryload(Customer.INVOICE)).filter_by(name = 'Govind Pant').one()


# SELECT "CUSTOMER".id AS "CUSTOMER_id", "CUSTOMER".name AS "CUSTOMER_name", "CUSTOMER".address AS "CUSTOMER_address", "CUSTOMER".email AS "CUSTOMER_email",
# "INVOICE_1".id AS "INVOICE_1_id", "INVOICE_1".custid AS "INVOICE_1_custid", "INVOICE_1".invno AS "INVOICE_1_invno", "INVOICE_1".amount AS "INVOICE_1_amount"
# FROM "CUSTOMER" LEFT OUTER JOIN "INVOICE" AS "INVOICE_1" ON "CUSTOMER".id = "INVOICE_1".custid WHERE "CUSTOMER".name = ? ORDER BY "INVOICE_1".id
from sqlalchemy.orm import joinedload
c1 = session.query(Customer).options(joinedload(Customer.INVOICE)).filter_by(name='Govind Pant').one()


# SELECT "CUSTOMER".id AS "CUSTOMER_id", "CUSTOMER".name AS "CUSTOMER_name", "CUSTOMER".address AS "CUSTOMER_address", "CUSTOMER".email AS "CUSTOMER_email"
# FROM "CUSTOMER" WHERE "CUSTOMER".id = 2

# SELECT "INVOICE".id AS "INVOICE_id", "INVOICE".custid AS "INVOICE_custid", "INVOICE".invno AS "INVOICE_invno", "INVOICE".amount AS "INVOICE_amount"
# FROM "INVOICE" WHERE 2 = "INVOICE".custid ORDER BY "INVOICE".id

# UPDATE "INVOICE" SET custid=? WHERE "INVOICE".id = ?

# DELETE FROM "CUSTOMER" WHERE "CUSTOMER".id = 2
x = session.query(Customer).get(2)
session.delete(x)


# SELECT count(*) AS count_1 FROM (SELECT "INVOICE".id AS "INVOICE_id", "INVOICE".custid AS "INVOICE_custid", "INVOICE".invno AS "INVOICE_invno", 
# "INVOICE".amount AS "INVOICE_amount" FROM "INVOICE" WHERE "INVOICE".invno IN (?, ?)) AS anon_1
session.query(Invoice).filter(Invoice.invno.in_([10,14])).count()

print (Invoice.custid.foreign_keys)
