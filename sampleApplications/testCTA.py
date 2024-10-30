import sys
import urllib
import datetime
import nzpy

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
import nzalchemy as nz

def creator():
    return nzpy.connect(user="admin", password="password",host='myhost', port=5480, database="db", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator, echo=True)

##Engine class methods
qry = "Select * from T1";
engine.execute("drop table t1 if exists")
engine.execute("create table t1(c1 int)")
result = engine.execute("select * from t1")
engine.execute("drop table testCTA if exists")
s = engine.execute("select * from \"TESTDT\"")
s1 = "select \"NZ_DATE\", \"NZ_DATETIME\" from \"TESTDT\""
engine.execute(nz.CreateTableAs('testCTA', s1, True, "random", '\"NZ_DATE\"')) #,False, "NZ_DATE, NZ_DATETIME", '(NZ_DATE, NZ_DATETIME)'))

for row in result:
    print (row)

##Metadata, Table Object and its methods
meta = MetaData()
tObj = Table(
   't1', meta,
   Column('id', BIGINT),
   Column('name', nz.NVARCHAR(10)),
   Column('gender', nz.NCHAR),
)
engine.execute("drop table t1 if exists") 
#meta.drop_all(engine)
meta.create_all(engine)
