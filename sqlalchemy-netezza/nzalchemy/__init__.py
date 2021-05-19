# This is the MIT license: http://www.opensource.org/licenses/mit-license.php
#
# Copyright (c) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>.
# SQLAlchemy is a trademark of Michael Bayer.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from sqlalchemy.dialects import registry as _registry

from .base import(
     BOOLEAN,
     BOOL,

     BIGINT,
     INTEGER,
     SMALLINT,
     INT,
     INT1,
     INT2,
     INT4,
     INT8,
     BYTEINT,

     VARCHAR,
     CHAR,
     BPCHAR,
     NCHAR,
     NVARCHAR,

     FLOAT,
     FLOAT4,
     FLOAT8,
     REAL,
     DOUBLE,
     DOUBLE_PRECISION,
     NUMERIC,
     DECIMAL,

     ST_GEOMETRY,
     VARBINARY,

     TIME,
     TIMESTAMP,
     DATETIME,
     TIMETZ,
     DATE,

     INTERVAL,

     OID,
     NAME,
     BYTEA,
     TEXT,
     ABSTIME,
    
     UNICODE,
 
     CreateTableAs,
)
'''
from .dml import Insert
from .dml import insert
from .ext import aggregate_order_by
from .hstore import HSTORE
from .hstore import hstore
from .json import JSON
from .json import JSONB
'''

#__version__ = "11.0.0"

_registry.register(
    "netezza.pyodbc", "nzalchemy.pyodbc", "NetezzaDialect_pyodbc"
)
_registry.register(
    "netezza.nzpy", "nzalchemy.nzpy", "NetezzaDialect_nzpy"
)

__all__ = (
    "BOOLEAN",
    "BOOL",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "INT",
    "INT1",
    "INT2",
    "INT4",
    "INT8",
    "BYTEINT",

    "VARCHAR",
    "CHAR",
    "BPCHAR",
    "NCHAR",
    "NVARCHAR",
    
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "REAL",
    "DOUBLE",
    "DOUBLE_PRECISION",
    "NUMERIC",
    "DECIMAL",

    "TIMESTAMP",
    "DATETIME",
    "TIME",
    "TIMETZ",
    "DATE",

    "ST_GEOMETRY",
    "VARBINARY",
  
    "INTERVAL",
    
    "OID",
    "NAME",
    "BYTEA",
    "TEXT",
    "ABSTIME",

    "UNICODE"
    
    "ARRAY",
    "dialect",
    "HSTORE",
    "hstore",
    "JSON",
    "JSONB",
    "Any",
    "All",
    "aggregate_order_by",
    "insert",
    "Insert",
    "CreateTableAs",
)

