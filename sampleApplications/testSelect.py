import os
import sys
import nzpy
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

print ("\n--------- " + sys.argv[0] + " ---------\n")

host = os.getenv("MY_HOST")
user = os.getenv("MY_USER")
password = os.getenv("MY_PASSWORD")
db = os.getenv("MY_DB")
port = os.getenv("MY_PORT")

def creator():
    return nzpy.connect(user=f"{user}", password=f"{password}",host=f"{host}", port=int(port), database=f"{db}", securityLevel=0,logOptions=nzpy.LogOptions.Logfile, char_varchar_encoding='utf8')
engine = create_engine("netezza+nzpy://", creator=creator) 
print (engine)

meta = MetaData()
TEST = Table(
   'TESTSELECT', meta,
   Column('id', BIGINT),
   Column('name', nz.NVARCHAR(2)),
   Column('gender', CHAR),
)
meta.drop_all(engine)
meta.create_all(engine)

conn = engine.connect()
conn.execute(TEST.insert().values(id='1',name='j1', gender='M'))
conn.execute(TEST.insert().values(id='2',name='j2', gender='M'))
conn.execute(TEST.insert().values(id='3',name='j3', gender='F'))
conn.execute(TEST.insert().values(id='4',name='j4', gender='F'))

col = Column('name')
col2 = Column('gender')

testselect = meta.tables['TESTSELECT']

query = select(
    func.count(),col2
).select_from(testselect).order_by(col2).group_by(col2)

va=10
result = conn.execute(query)
for row in result:
    print (row)


