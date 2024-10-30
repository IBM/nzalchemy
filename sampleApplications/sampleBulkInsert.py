import os
import sys
import urllib
import datetime
import nzalchemy as nz
import csv
import nzpy
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

session.bulk_save_objects(
        [
            Customers(
                name="customer name %d" % i,
                address="address %d" %i,
                email="email %d" %i,
            )
            for i in range(100)
        ]
)
session.commit()

session.bulk_insert_mappings(
        Customers,
        [
            dict(
                name="customer name %d" % i,
                address="address %d" %i,
                email="email %d" %i,
            )
            for i in range(10000)
        ],
)
session.commit()

buffer = []

with open('million_users.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    for row in csv_reader:
        buffer.append({
            'name': row[0],
            'address': row[1],
            'email': row[2],
        })
        if len(buffer) % 10000 == 0:
            session.bulk_insert_mappings(Customers,buffer)
            buffer = []

session.bulk_insert_mappings(Customers, buffer)
session.commit()

res = session.query(Customers).count()
print (res)

result = session.query(Customers).all()
for row in result:
   print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)

