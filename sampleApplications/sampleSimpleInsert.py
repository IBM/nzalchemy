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
import csv
from sqlalchemy.orm import Session

##Engine Creation
engine = create_engine("netezza+nzpy://admin:password@localhost:5480/db1")

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

#INSERT
for i in range(10000):
    session = Session(bind=engine)
    session.add(
        Customers(
            name="customer name %d" % i,
            address="customer address %d" % i,
            email="email %d" %i,
            
        )
    )
    session.commit()


session = Session(bind=engine)
for i in range(10000):
    session.add_all(
        [
            Customers(
                name="customer name %d" % i,
                address="customer address %d" % i,
                email="email %d" %i,
            )
        ]
    )
session.commit()


session.add_all([
   Customers(id = 2, name = 'Komal Pande', address = 'Koti, Hyderabad', email = 'komal@gmail.com'), 
   Customers(id = 3, name = 'Rajender Nath', address = 'Sector 40, Gurgaon', email = 'nath@gmail.com'), 
   Customers(id = 4, name = 'S.M.Krishna', address = 'Budhwar Peth, Pune', email = 'smk@gmail.com')]
)
session.commit()

res = session.query(Customers).count()
print (res)

result = session.query(Customers).all()
for row in result:
   print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)

