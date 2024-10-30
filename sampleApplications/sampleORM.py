import os
import sys
import urllib
import datetime
import nzalchemy as nz
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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

print ("\n--------- " + sys.argv[0] + " ---------\n")

host = os.getenv("MY_HOST")
user = os.getenv("MY_USER")
password = os.getenv("MY_PASSWORD")
db = os.getenv("MY_DB")
port = os.getenv("MY_PORT")

def creator():
    return nzpy.connect(user=f"{user}", password=f"{password}",host=f"{host}", port=int(port), database=f"{db}", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator) 

Base = declarative_base()
class Customers(Base):
   __tablename__ = 'CUSTOMERS'
   id = Column(nz.SMALLINT, Sequence('USR_ID_SEQ3'), primary_key = True)
   name = Column(VARCHAR(30))
   address = Column(nz.NVARCHAR(30))
   email = Column(nz.NCHAR(30))

Customers.__table__.drop(engine, checkfirst=True)
Customers.__table__.create(engine, checkfirst=True)

Session = sessionmaker(bind = engine)
session = Session()

#INSERT
c1 = Customers(name = 'Ravi Kumar', address = 'Station Road Nanded', email = 'ravi@gmail.com') #Error : without mentioning id,base.py: get_insert_default()
session.add(c1)
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

row = session.query(Customers).get(2)
print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)
row = session.query(Customers).first()
print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)

session.query(Customers).filter(Customers.id != 2).update({Customers.name:"Mr."+Customers.name}, synchronize_session = False)

result = session.query(Customers).filter(Customers.id>2)
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)

result = session.query(Customers).filter(Customers.name.like('Ra%'))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)

from sqlalchemy import literal
search_string = "asasd"
result = session.query(Customers).filter(literal(search_string).contains(Customers.name))

from sqlalchemy.sql import func
exif_conditions = [func.substr("asdasdasdasd", 7, 4) == b'Exif']

result = session.query(Customers).filter(Customers.id.in_([1,3]))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)

from sqlalchemy import or_
result = session.query(Customers).filter(or_(Customers.id>2, Customers.name.like('Ra%')))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)

session.query(Customers).filter(Customers.id == 3).scalar()

from sqlalchemy import text
for cust in session.query(Customers).filter(text("id<3")):
   print(cust.name)

print (session.query(Customers).column_descriptions)

print (Customers.id.foreign_keys)

from sqlalchemy.engine import reflection
insp = reflection.Inspector.from_engine(engine)
