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
#from sqlalchemy import select
import urllib
import datetime
import nzalchemy as nz
import nzpy
def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator) #working
# ##Engine Creation
# params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)


from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
class Customers(Base):
   __tablename__ = 'CUSTOMERS'
   
   #id = Column(Integer, primary_key = True)
   id = Column(nz.SMALLINT, Sequence('USR_ID_SEQ3'), primary_key = True)
   #id = Column(nz.BIGINT, Sequence('USR_ID_SEQ1'), primary_key = True)
   name = Column(VARCHAR(30))
   address = Column(nz.NVARCHAR(30))
   email = Column(nz.NCHAR(30))

#Base.metadata.drop_all(engine, tables=[Customers.__tablename__],checkfirst=True)
#Base.metadata.create_all(engine, checkfirst=True)

Customers.__table__.drop(engine, checkfirst=True)
Customers.__table__.create(engine, checkfirst=True)

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind = engine)
session = Session()

#INSERT
c1 = Customers(name = 'Ravi Kumar', address = 'Station Road Nanded', email = 'ravi@gmail.com') #Error : without mentioning id,base.py: get_insert_default()
#c1 = Customers(id = 1 ,name = 'Ravi Kumar', address = 'Station Road Nanded', email = 'ravi@gmail.com')
session.add(c1)
session.commit()

session.add_all([
   Customers(id = 2, name = 'Komal Pande', address = 'Koti, Hyderabad', email = 'komal@gmail.com'), 
   Customers(id = 3, name = 'Rajender Nath', address = 'Sector 40, Gurgaon', email = 'nath@gmail.com'), 
   Customers(id = 4, name = 'S.M.Krishna', address = 'Budhwar Peth, Pune', email = 'smk@gmail.com')]
)
session.commit()

#SELECT
#q = session.query(mapped class) #Query object
#q = Query(mappedClass, session) #same as above

res = session.query(Customers).count()
print (res)

result = session.query(Customers).all()
for row in result:
   print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)

row = session.query(Customers).get(2)
print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)
#row.name = 'Ravi Shrivastava'
row = session.query(Customers).first() #Error, limit clause issue
print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)

#filter and update
session.query(Customers).filter(Customers.id != 2).update({Customers.name:"Mr."+Customers.name}, synchronize_session = False)

result = session.query(Customers).filter(Customers.id>2)
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)

result = session.query(Customers).filter(Customers.name.like('Ra%'))
for row in result:
   print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)

from sqlalchemy import literal
search_string = "asasd" #['ed', 'wendy', 'jack']
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
#session.query(Customers).one() #fails as more than 1 row

from sqlalchemy import text
for cust in session.query(Customers).filter(text("id<3")):
   print(cust.name)

print (session.query(Customers).column_descriptions)

print (Customers.id.foreign_keys)


from sqlalchemy.engine import reflection
insp = reflection.Inspector.from_engine(engine)
#print(insp.get_table_comment('CUSTOMERS'))
