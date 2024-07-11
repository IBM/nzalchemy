import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, select, desc, text
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

from sqlalchemy import literal_column
from sqlalchemy import literal

##Engine Creation
# params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator,echo=True) #working

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
engine.connect().execute(text("drop table t1 if exists")) 
#meta.drop_all(engine)
meta.create_all(engine)

# engine.connect().execute(tObj.insert())
engine.connect().execute(tObj.insert().values(id = 1,name='ayush',gender='M'))
print('after updation')
# engine.connect().execute(tObj.insert().values(id=2))
