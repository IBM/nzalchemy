import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
import pg8000
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, select, desc
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
params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)

#engine = create_engine("postgres+pg8000://postgres@localhost:5432/db1", echo=True) #working
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
print (engine)

meta = MetaData()
TEST = Table(
   'TESTSELECT', meta,
   Column('id', BIGINT),
   #Column('id', BIGINT, primary_key = True),
#   Column('name', VARCHAR(20) ),
   #Column('name', nz.NVARCHAR(20) ),
   Column('name', nz.NVARCHAR(2)),
   #Column('name', nz.NCHAR(20) ),
   Column('gender', CHAR),
   #Column('gender', nz.NCHAR),
)
meta.drop_all(engine)
meta.create_all(engine)

conn = engine.connect()
conn.execute(TEST.insert().values(id='3',name='j', gender='M'))
conn.execute(TEST.insert().values(id='3',name='j', gender='M'))

#meta.drop_all(engine)
col = Column('name')
col2 = Column('gender')
#s = select([TEST])#.where(col=='jack1')
#result = conn.execute(s)
#for row in result:
#   print (row)
#s = select([TEST]).where(col=='jack1').order_by(desc(col2)).limit(10)

s = select([TEST]).where(col=='j').order_by(col2)

va=10
s = select([TEST]).where(col=='j1').limit(2)
result = conn.execute(s)
for row in result:
    print (row)


