import os
import sys
import urllib
import nzpy
import nzalchemy as nz
from sqlalchemy import create_engine, MetaData, Table, Column, text

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
   'TESTDT', meta,
Column("NZ_BOOLEAN",nz.BOOLEAN),
Column("NZ_BOOL",nz.BOOL),

Column("NZ_BIGINT",nz.BIGINT),
Column("NZ_INTEGER",nz.INTEGER),
Column("NZ_SMALLINT",nz.SMALLINT),
Column("NZ_INT",nz.INT),
Column("NZ_INT1",nz.INT1),
Column("NZ_INT2",nz.INT2),
Column("NZ_INT4",nz.INT4),
Column("NZ_INT8",nz.INT8),
Column("NZ_BYTEINT",nz.BYTEINT),

Column("NZ_CHAR",nz.CHAR),
Column("NZ_BPCHAR",nz.BPCHAR),
Column("NZ_VARCHAR",nz.VARCHAR(100)),
Column("NZ_NCHAR1",nz.NCHAR),
Column("NZ_NCHAR10",nz.NCHAR(10)),
Column("NZ_NVARCHAR1",nz.NVARCHAR(1)),
Column("NZ_NVARCHAR10",nz.NVARCHAR(10)),

Column("NZ_FLOAT",nz.FLOAT(11)),
Column("NZ_FLOAT4",nz.FLOAT4),
Column("NZ_FLOAT8",nz.FLOAT8),
Column("NZ_REAL",nz.REAL),
Column("NZ_DOUBLE",nz.DOUBLE),
Column("NZ_DOUBLE_PRECISION",nz.DOUBLE_PRECISION),
Column("NZ_DECIMAL",nz.DECIMAL),
Column("NZ_DECIMAL10_2",nz.DECIMAL(10,2)),
Column("NZ_NUMERIC",nz.NUMERIC),
Column("NZ_NUMERIC10_2",nz.NUMERIC(10,2)),

Column("NZ_DATE",nz.DATE),
Column("NZ_TIMESTAMP",nz.TIMESTAMP),
Column("NZ_DATETIME",nz.DATETIME),
Column("NZ_TIMETZ",nz.TIMETZ),
Column("NZ_TIME",nz.TIME),

Column("NZ_VARBINARY",nz.VARBINARY(20)),
Column("NZ_ST_GEOMETRY",nz.ST_GEOMETRY(20)),

Column("NZ_INTERVAL", nz.INTERVAL),

)
meta.drop_all(engine)
meta.create_all(engine)

conn = engine.connect()
conn.execute(TEST.insert().values(
NZ_BOOLEAN= 0 ,
NZ_BOOL=  False,

NZ_BIGINT=  10,
NZ_INTEGER=  12,
NZ_SMALLINT=  12,
NZ_INT= 3 ,
NZ_INT1=  1,
NZ_INT2= 23 ,
NZ_INT4=  23,
NZ_INT8=  12,
NZ_BYTEINT=  7,

NZ_CHAR=  '2',
NZ_BPCHAR=  '6',
NZ_VARCHAR=  "23",
NZ_NCHAR1= '2' ,
NZ_NCHAR10= "wewe" ,
NZ_NVARCHAR1= 'w' ,
NZ_NVARCHAR10= "we" ,

NZ_FLOAT= 12.2 ,
NZ_FLOAT4= 23.2 ,
NZ_FLOAT8= 24.5 ,
NZ_REAL= 12.12 ,
NZ_DOUBLE= 34.3 ,
NZ_DOUBLE_PRECISION= 34.3 ,
NZ_DECIMAL=  34,
NZ_DECIMAL10_2= 34 ,
NZ_NUMERIC= 34 ,
NZ_NUMERIC10_2= 34 ,

NZ_DATE= '1991-1-1',
NZ_TIMESTAMP= '2016-11-11 03:59:08.8642',
NZ_DATETIME= '2016-11-11 03:59:08.8642',
NZ_TIMETZ= '12:57:42+05:30',
NZ_TIME= '12:19:23',
NZ_VARBINARY= '68656c6c6f'.encode('utf-8'),
NZ_ST_GEOMETRY= '68656c6c6f'.encode('utf-8'),
NZ_INTERVAL= '1y4mon3d23:34:57.232',

))

result = conn.execute(text("SELECT * FROM TESTDT"))
for row in result:
    print (row)

from sqlalchemy import inspect
inspector = inspect(engine)
result = inspector.get_table_names()
result = inspector.get_foreign_table_names()
for row in result:
    print (row)


