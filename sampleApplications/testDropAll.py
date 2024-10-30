import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
import pg8000
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
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
params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)

engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
print (engine)

meta = MetaData()
TEST = Table(
   'TESTDROP', meta,
   Column('id', BIGINT),
   Column('name', VARCHAR(20) ),
   Column('gender', CHAR),
)
meta.create_all(engine)

conn = engine.connect()
conn.execute(TEST.insert().values(id='1',name='jack1', gender='M'))

meta.drop_all(engine)
#conn.execute(TEST.delete())
