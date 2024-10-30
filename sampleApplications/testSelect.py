import sys
import urllib
import datetime
import nzpy
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
import nzalchemy as nz

def creator():
    return nzpy.connect(user="admin", password="password",host='myhost', port=5480, database="db", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator)
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

testselect = meta.tables['TESTSELECT']
# SQLAlchemy Query to ORDER BY and GROUP BY 
query = select(
    func.count(),col2
).select_from(testselect).order_by(col2).group_by(col2)

va=10
result = conn.execute(query)
for row in result:
    print (row)


