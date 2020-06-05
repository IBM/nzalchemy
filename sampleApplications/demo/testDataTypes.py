import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
from sqlalchemy import create_engine, MetaData, Table, Column

import nzalchemy as nz
import urllib
params = urllib.parse.quote_plus("DRIVER=NetezzaSQL;SERVER=longpassword1.fyre.ibm.com;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
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

)
meta.drop_all(engine)
meta.create_all(engine)

conn = engine.connect()
conn.execute(TEST.insert().values(
NZ_BOOLEAN= 0 ,
NZ_BOOL=  0,

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
NZ_TIMETZ= '12:57:42+05:30',  #Expects in +/- hh:mm format
NZ_TIME= '12:19:23',
NZ_VARBINARY= bytes('0x68656c6c6f','utf-8'),
NZ_ST_GEOMETRY= bytes('0x68656c6c6f','utf-8'),

))

result = conn.execute(TEST.select())
for row in result:
    print (row)


