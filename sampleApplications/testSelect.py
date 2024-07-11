import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
import pg8000
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, select, desc , text , func
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
# params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
# print(params)

#engine = create_engine("postgres+pg8000://postgres@localhost:5432/db1", echo=True) #working
# engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
def creator():
    return nzpy.connect(user="admin", password="password",host='ayush-nps-server1.fyre.ibm.com', port=5480, database="dev_ayush", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator) #working
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
conn.execute(TEST.insert().values(id='1',name='j1', gender='M'))
conn.execute(TEST.insert().values(id='2',name='j2', gender='M'))
conn.execute(TEST.insert().values(id='3',name='j3', gender='F'))
conn.execute(TEST.insert().values(id='4',name='j4', gender='F'))

#meta.drop_all(engine)
col = Column('name')
col2 = Column('gender')
#s = select([TEST])#.where(col=='jack1')
#result = conn.execute(s)
#for row in result:
#   print (row)
#s = select([TEST]).where(col=='jack1').order_by(desc(col2)).limit(10)

# s = select(count(TEST)).order_by(col2)
testselect = meta.tables['TESTSELECT']
# SQLAlchemy Query to ORDER BY and GROUP BY 
query = select(
    func.count(),col2
).select_from(testselect).order_by(col2).group_by(col2)

va=10
# s = select(TEST).where(col=='j').limit(2)
result = conn.execute(query)
for row in result:
    print (row)


