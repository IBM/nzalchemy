'''
Test data:

create table t2(c1 float4, c2 double, c3 int1, c4 int2, c5 int4, c6 int8, c7 char(5), c8 varchar(10), DATE_PROD DATE, TIME_PROD TIME, INTERVAL_PROD INTERVAL,TIMESTMP TIMESTAMP,TIMETZ_PROD TIME WITH TIME ZONE, c18 bool);
  
insert into t2 values (-1,-1,-1,-1,-1,-1,'','', '1991-1-1', '12:19:23', '1y4mon3d23h34m67s234565ms', '2016-11-11 03:59:08.8642','12:57:42 AEST', 'yes');
 
insert into t2 values (-123.345, -23456.789, -128, -32768, -234567, -45678923,'xyz', 'Go lang', '2001-12-31', '23:59:59', '5y6mon7d23h21m67s897ms', '1996-12-11 23:59:08.1232','10:47:53 BST', 'false');

create table PrimaryKey_demo1 ( col1 smallint NOT NULL PRIMARY KEY ,col2 date ,col3 varchar(60 ) ) Distribute on (col1);

CREATE TABLE PrimaryKey_demo2
(
 col1 smallint NOT NULL
 ,colref2 smallint 
 ,col3 varchar(300 )
 ,CONSTRAINT fk_column_colref2 
 FOREIGN KEY (colref2) 
 REFERENCES PrimaryKey_demo1 (col1) 
) distribute on (col1);

create table test2delete (
        col1 smallint
       ,col3 varchar(30 )
);
alter table test2delete add constraint uk_test2delete unique (col1);

CREATE VIEW t2_view AS SELECT c5, c1,c4 DATE_PROD,TIME_PROD,TIMETZ_PROD  FROM t2;

'''

from sqlalchemy import create_engine, MetaData, Table, Column

import sqlalchemy_nz as nz
import urllib
import pdb
params = urllib.parse.quote_plus("DRIVER=NetezzaSQL;SERVER=longpassword1.fyre.ibm.com;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)
engine = create_engine("nz+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
print (engine)
 
from sqlalchemy import inspect
inspector = inspect(engine)
 
result = inspector.get_table_oid("T2")
print (result)
 
result = inspector.get_schema_names()
for row in result:
    print (row)
 
result = inspector.get_table_names()
for row in result:
    print (row)
 
result = inspector.get_foreign_table_names()
for row in result:
    print (row)
 
result = inspector.get_view_names()
for row in result:
    print (row)
 
result = inspector.get_view_definition("T2_VIEW")
print (result)
 
result = inspector.get_columns("T2")
for row in result:
    print (row)
 
result = inspector.get_pk_constraint("PrimaryKey_demo1")
for row in result:
    print (row)
 
result = inspector.get_foreign_keys("PrimaryKey_demo2")
for row in result:
    print (row)
 
result = inspector.get_unique_constraints("test2delete")
for row in result:
    print (row)
 
''' 

result = inspector.get_indexes()
for row in result:
    print (row)

result = inspector.get_table_comment()
for row in result:
    print (row)
 
result = inspector.get_check_constraints()
for row in result:
    print (row) 
'''