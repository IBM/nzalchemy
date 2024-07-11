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
import nzpy

def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator, echo=True) #working

##Engine class methods
qry = "Select * from T1";
engine.execute("drop table t1 if exists") #Scuccess: QUERY: Select * from T1 at netezza
engine.execute("create table t1(c1 int)") #Scuccess: QUERY: Select * from T1 at netezza
result = engine.execute("select * from t1") #Scuccess: QUERY: Select * from T1 at netezza
engine.execute("drop table testCTA if exists")
#engine.execute("create table testCTA as select * from testdt distribute on 	random organize on (NZ_DATE, NZ_DATETIME);") #Scuccess: QUERY: Select * from T1 at netezza
s = engine.execute("select * from \"TESTDT\"")
s1 = "select \"NZ_DATE\", \"NZ_DATETIME\" from \"TESTDT\""
#engine.execute(nz.CreateTableAs('testCTA', s1,False, "NZ_DATE, NZ_DATETIME", '(NZ_DATE, NZ_DATETIME)'))
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

