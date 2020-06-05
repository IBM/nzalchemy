import sys
print ("\n--------- " + sys.argv[0] + " ---------\n")
from sqlalchemy import create_engine, MetaData, Table, Column
'''
import pg8000
from sqlalchemy.dialects import postgresql as ps
engine = create_engine("postgres+pg8000://postgres@localhost:5432/db1", echo=True) #working
'''

import nzalchemy as nz
import urllib
params = urllib.parse.quote_plus("DRIVER=/nzscratch/spawar72/SQLAlchemy/ODBC/lib64/libnzodbc.so;SERVER=172.16.34.147;PORT=5480;DATABASE=TESTODBC;UID=admin;PWD=password")
print(params)
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True) #working
print (engine)

from sqlalchemy import inspect
inspector = inspect(engine)

result = inspector.get_table_oid(123)
for row in result:
    print (row)

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

result = inspector.get_view_definition()
for row in result:
    print (row)

result = inspector.get_columns()
for row in result:
    print (row)

result = inspector.get_pk_constraint()
for row in result:
    print (row)

result = inspector.get_foreign_keys()
for row in result:
    print (row)

result = inspector.get_indexes()
for row in result:
    print (row)

result = inspector.get_unique_constraints()
for row in result:
    print (row)

result = inspector.get_table_comment()
for row in result:
    print (row)

result = inspector.get_check_constraints()
for row in result:
    print (row)

