import sys
import urllib

print ("\n--------- " + sys.argv[0] + " ---------\n")

import pg8000
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
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


params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)

engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
print (engine)

meta = MetaData()
TEST = Table(
   'TESTCREATE1', meta,
   Column('id', BIGINT),
   Column('name', VARCHAR(20) ),
   Column('gender', String),  #It fails
)
meta.create_all(engine)


