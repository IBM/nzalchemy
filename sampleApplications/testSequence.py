import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
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

from sqlalchemy import literal_column
from sqlalchemy import literal

##Engine Creation
params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)

##Engine class methods

##Metadata, Table Object and its methods
meta = MetaData()
tObj = Table(
   't1', meta,
   Column('id', nz.INTEGER, primery_key=True),
   Column('name', nz.NVARCHAR(10)),
   Column('gender', nz.NCHAR),
   Column('id2', nz.INT, default=literal_column("2", type_=Integer) + literal(2))
)
engine.execute("drop table t1 if exists") 
#meta.drop_all(engine)
meta.create_all(engine)

engine.connect().execute(tObj.insert())
#engine.connect().execute(tObj.insert().values(gender='2'))
#engine.connect().execute(tObj.insert().values(id=2))
