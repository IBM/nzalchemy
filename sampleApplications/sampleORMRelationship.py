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
import nzpy

##Engine Creation
# params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator) #working


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


for c, i in session.query(Customer, Invoice).filter(Customer.id == Invoice.custid).all():
   print ("ID: {} Name: {} Invoice No: {} Amount: {}".format(c.id,c.name, i.invno, i.amount))

result = session.query(Customer).join(Invoice).filter(Invoice.amount == 8500)
for row in result:
   for inv in row.INVOICE:
      print (row.id, row.name, inv.invno, inv.amount)

from sqlalchemy.sql import func
stmt = session.query(
   Invoice.custid, func.count('*').label('invoice_count')
).group_by(Invoice.custid).subquery()

for u, count in session.query(Customer, stmt.c.invoice_count).outerjoin(stmt, Customer.id == stmt.c.custid).order_by(Customer.id):
   print(u.name, count)

s = session.query(Customer).filter(Invoice.invno.__eq__(12))

s = session.query(Invoice).filter(Invoice.customer.has(name = 'Arjun Pandit'))

s = session.query(Invoice).filter(Invoice.invno.contains([3,4,5]))

s = session.query(Customer).filter(Customer.INVOICE.any(Invoice.invno==11))

from sqlalchemy.orm import subqueryload
c1 = session.query(Customer).options(subqueryload(Customer.INVOICE)).filter_by(name = 'Govind Pant').one()

from sqlalchemy.orm import joinedload
#c1 = session.query(Customer).options(joinedload(Customer.INVOICE)).filter_by(name='Govind Pant').one()


x = session.query(Customer).get(2)
session.delete(x)

session.query(Invoice).filter(Invoice.invno.in_([10,14])).count()
print (Invoice.custid.foreign_keys)

#print (Invoice.get_table_comment())
