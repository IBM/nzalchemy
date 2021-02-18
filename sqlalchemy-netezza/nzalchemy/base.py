# nzalchemy/base.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

r"""

SQLAlchemy Dialect for Netezza

Auto Increment Behavior
-----------------------
SQLAlchemy Table objects which include integer primary keys are usually
assumed to have "autoincrementing" behavior, meaning they can generate their
own primary key values upon INSERT.  Since Netezza has no "autoincrement"
feature, SQLAlchemy relies upon sequences to produce these values.   With the
Netezza dialect, *a sequence must always be explicitly specified to enable
autoincrement*. To specify sequences, use the sqlalchemy.schema.Sequence object
which is passed to a Column construct::

  t = Table('mytable', metadata,
        Column('id', Integer, Sequence('id_seq'), primary_key=True),
        Column(...), ...
  )

This step is also required when using table reflection, i.e. autoload=True::

  t = Table('mytable', metadata,
        Column('id', Integer, Sequence('id_seq'), primary_key=True),
        autoload=True
  )

"""
from collections import defaultdict
import datetime as dt
import re

from sqlalchemy import exc
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy.sql import compiler
from sqlalchemy.sql import elements
from sqlalchemy.sql import expression
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import util as sql_util
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import compiles

from sqlalchemy.types import BOOLEAN
from sqlalchemy.types import BIGINT
from sqlalchemy.types import SMALLINT
from sqlalchemy.types import INTEGER
from sqlalchemy.types import CHAR
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import FLOAT
from sqlalchemy.types import REAL
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import DECIMAL
from sqlalchemy.types import DATE
from sqlalchemy.types import VARBINARY
from sqlalchemy.types import TEXT

def getFileName():
    # Log file created in application directory
    logFile = 'nzalchemy.log'

    from datetime import date
    today = date.today()
    # ddmmYY
    td = today.strftime("%d%m%Y")
    logFileDay = logFile+td # Everytime log file will be appended to days log file

    import os
    if os.path.exists(logFile):
        fin = open(logFile, "r")
        dat = fin.read()
        fin.close()
        fout = open(logFileDay, "a")
        fout.write(dat)
        fout.close()
        os.remove(logFile)
    return logFile

import logging as log
LOG_FILENAME = getFileName()

try:
    log.basicConfig(level=log.DEBUG, 
                    filename=LOG_FILENAME,
                    format="%(asctime)-15s [%(levelname)5s] %(filename)10s %(funcName)25s() %(lineno)4s : %(message)s")
except Exception as e: # if error in file creation, log only info messaged to console
    log.basicConfig(level=log.INFO,
                    filename="",
                    format="%(asctime)-15s [%(levelname)5s] %(filename)10s %(funcName)25s() %(lineno)4s : %(message)s")
#referenced
AUTOCOMMIT_REGEXP = re.compile(
    r"\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER|GRANT|REVOKE|"
    "IMPORT FOREIGN SCHEMA|REFRESH MATERIALIZED VIEW|TRUNCATE)",
    re.I | re.UNICODE,
)

#referenced
RESERVED_WORDS = set(
    [
        "all",
        "analyse",
        "analyze",
        "and",
        "any",
        "as",
        "asc",
        "asymmetric",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "column",
        "constraint",
        "create",
        "current_catalog",
        "current_date",
        "current_role",
        "current_time",
        "current_timestamp",
        "current_user",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "fetch",
        "for",
        "foreign",
        "from",
        "grant",
        "group",
        "having",
        "in",
        "initially",
        "intersect",
        "into",
        "leading",
        "limit",
        "localtime",
        "localtimestamp",
        "new",
        "not",
        "null",
        "of",
        "off",
        "offset",
        "old",
        "on",
        "only",
        "or",
        "order",
        "placing",
        "primary",
        "references",
        "returning",
        "select",
        "session_user",
        "some",
        "symmetric",
        "table",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "variadic",
        "when",
        "where",
        "window",
        "with",
        "authorization",
        "between",
        "binary",
        "cross",
        "current_schema",
        "freeze",
        "full",
        "ilike",
        "inner",
        "is",
        "isnull",
        "join",
        "left",
        "like",
        "natural",
        "notnull",
        "outer",
        "over",
        "overlaps",
        "right",
        "similar",
        "verbose",
    ]
)

class BOOL(sqltypes.TypeEngine):
    __visit_name__ = "BOOL"

class INT(sqltypes.TypeEngine):
    __visit_name__ = "INT"

class INT1(sqltypes.TypeEngine):
    __visit_name__ = "INT1"

class INT2(sqltypes.TypeEngine):
    __visit_name__ = "INT2"

class INT4(sqltypes.TypeEngine):
    __visit_name__ = "INT4"

class INT8(sqltypes.TypeEngine):
    __visit_name__ = "INT8"

class BYTEINT(sqltypes.TypeEngine):
    __visit_name__ = "BYTEINT"

class BPCHAR(sqltypes.TypeEngine):
    __visit_name__ = "BPCHAR"

class NCHAR(sqltypes.TypeEngine):
    __visit_name__ = "NCHAR"
    def __init__(self, length=None, varying=False):
        log.debug("-->")
        if not varying:
            # NCHAR without VARYING defaults to length 1
            self.length = length or 1
        else:
            # but NCHAR VARYING can be unlimited-length, so no default
            self.length = length
        self.varying = varying

class NVARCHAR(sqltypes.NVARCHAR):
    def __init__(self, length=None, collation=None,
                 unicode_error=None):
        super(NVARCHAR, self).__init__(
            length,
            collation=collation,
            )

class FLOAT4(sqltypes.TypeEngine):
    __visit_name__ = "FLOAT4"

class FLOAT8(sqltypes.TypeEngine):
    __visit_name__ = "FLOAT8"

class DOUBLE(sqltypes.Float):
    __visit_name__ = "DOUBLE_PRECISION"

class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = "DOUBLE_PRECISION"

class TIMESTAMP(sqltypes.TIMESTAMP):
    __visit_name__ = "TIMESTAMP"

class DATETIME(sqltypes.TIMESTAMP):
    __visit_name__ = "TIMESTAMP"

class TIME(sqltypes.TIME):
    def __init__(self, timezone=False, precision=None):
        log.debug("-->")
        super(TIME, self).__init__(timezone=timezone)
        self.precision = precision

class TIMETZ(sqltypes.TypeEngine):
    __visit_name__ = "TIMETZ"

class ST_GEOMETRY(sqltypes.TypeEngine):
    __visit_name__ = "ST_GEOMETRY"
    def __init__(self, length=None, varying=False):
        log.debug("-->")
        if not varying:
            self.length = length or 1
        else:
            self.length = length
        self.varying = varying

class INTERVAL(sqltypes.TypeEngine):
    __visit_name__ = "INTERVAL"

#System Specific
class OID(sqltypes.TypeEngine):
    __visit_name__ = "OID"

class NAME(sqltypes.TypeEngine):
    __visit_name__ = "NAME"

class BYTEA(sqltypes.LargeBinary):
    __visit_name__ = "BYTEA"

class ABSTIME(sqltypes.TypeEngine):
    __visit_name__ = "ABSTIME"

class UNICODE(sqltypes.TypeEngine):
    __visit_name__ = "UNICODE"

#referenced
colspecs = {sqltypes.Interval: INTERVAL}

#referenced
ischema_names = {
    "boolean": BOOLEAN,
    "boolean": BOOL,
    "integer": INTEGER,
    "bigint": BIGINT,
    "smallint": SMALLINT,
    "integer": INT,
    "byteint": INT1,
    "smallint": INT2,
    "integer": INT4,
    "bigint": INT8,
    "byteint": BYTEINT,

    "character varying": VARCHAR,
    "character": CHAR,
    "character": BPCHAR,
    "national character": NCHAR,
    "national character varying": NVARCHAR,

    "float": FLOAT,
    "float4": FLOAT4,
    "float8": FLOAT8,
    "real": REAL,
    "double precision": DOUBLE,
    "double precision": DOUBLE_PRECISION,
    "numeric": NUMERIC,
    "decimal": DECIMAL,

    "st_geometry": ST_GEOMETRY,
    "varbinary" : VARBINARY,

    "date": DATE,
    "timestamp": TIMESTAMP,
    "timestamp": DATETIME,
    "time with time zone": TIMETZ,
    "time": TIME,
    "time with time zone": TIME,
    "time without time zone": TIME,

    "interval": INTERVAL,

    "oid": OID,
    "name": NAME,
    "bytea": BYTEA,
    "text": TEXT,
    "abstime": ABSTIME,

    "varying character": UNICODE,

    '"char"': sqltypes.String,
    "name": sqltypes.String,
}

class NetezzaCompiler(compiler.SQLCompiler):
    #referenced
    def limit_clause(self, select, **kw):
        log.debug("-->")
        '''Netezza doesn't allow sql params in the limit/offset piece'''
        text = ""
        if select._limit is not None:
            text += " \n LIMIT {limit}".format(limit=int(select._limit))
            log.debug("_limit: " + text)
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT ALL"
            text += " OFFSET {offset}".format(offset=int(select._offset))
        return text

    #referenced #Issue
    def get_select_precolumns(self, select, **kw):
        log.debug("-->")
        if select._distinct is not False:
            log.debug("Distinct: True")
            log.debug(select._distinct)
            if select._distinct is True:
                return "DISTINCT "
            elif isinstance(select._distinct, (list, tuple)): #Case might not be useful
                log.debug("Distinct isinstance")
                return (
                    "DISTINCT ON ("
                    + ", ".join(
                        [self.process(col, **kw) for col in select._distinct]
                    )
                    + ") "
                )
            else: #Case might not be useful
                log.debug("Distinct isinstance False") 
                return (
                    "DISTINCT ON ("
                    + self.process(select._distinct, **kw)
                    + ") "
                )
        else:
            log.debug("Distinct False")
            return ""

    #referenced
    def update_from_clause(
        self, update_stmt, from_table, extra_froms, from_hints, **kw
    ):
        log.debug("-->")
        return "FROM " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    #referenced
    def delete_extra_from_clause(
        self, delete_stmt, from_table, extra_froms, from_hints, **kw
    ):
        log.debug("-->")
        return ""

    #check in json implementation 	
    def visit_json_getitem_op_binary(
        self, binary, operator, _cast_applied=False, **kw
    ):
        log.debug("-->")
        if (
            not _cast_applied
            and binary.type._type_affinity is not sqltypes.JSON
        ):
            kw["_cast_applied"] = True
            return self.process(sql.cast(binary, binary.type), **kw)

        kw["eager_grouping"] = True

        return self._generate_generic_binary(
            binary, " -> " if not _cast_applied else " ->> ", **kw
        )

    #check in json implementation 	
    def visit_json_path_getitem_op_binary(
        self, binary, operator, _cast_applied=False, **kw
    ):
        log.debug("-->")
        if (
            not _cast_applied
            and binary.type._type_affinity is not sqltypes.JSON
        ):
            kw["_cast_applied"] = True
            return self.process(sql.cast(binary, binary.type), **kw)

        kw["eager_grouping"] = True
        return self._generate_generic_binary(
            binary, " #> " if not _cast_applied else " #>> ", **kw
        )

    #referenced in test
    def visit_empty_set_expr(self, element_types):
        log.debug("-->")
        # cast the empty set to the type we are comparing against.  if
        # we are comparing against the null type, pick an arbitrary
        # datatype for the empty set
        return "SELECT %s WHERE 1!=1" % (
            ", ".join(
                "CAST(0 AS %s)"
                % self.dialect.type_compiler.process(
                    INTEGER() if type_._isnull else type_
                )
                for type_ in element_types or [INTEGER()]
            ),
        )
    
    #referenced
    def render_literal_value(self, value, type_):
        log.debug("-->")
        value = super(NetezzaCompiler, self).render_literal_value(value, type_)

        if self.dialect._backslash_escapes:
            value = value.replace("\\", "\\")
        return value

    #referenced in test
    def visit_sequence(self, seq, **kw):
        log.debug("-->")
        return "next value for %s" % self.preparer.format_sequence(seq)

    #referenced #NotRequired
    def visit_substring_func(self, func, **kw):
        log.debug("-->")
        s = self.process(func.clauses.clauses[0], **kw)
        start = self.process(func.clauses.clauses[1], **kw)
        if len(func.clauses.clauses) > 2:
            length = self.process(func.clauses.clauses[2], **kw)
            return "SUBSTRING(%s FROM %s FOR %s)" % (s, start, length)
        else:
            return "SUBSTRING(%s FROM %s)" % (s, start)

class NetezzaDDLCompiler(compiler.DDLCompiler):
    #referenced #Check if case (not sure of having this case through sqlalchemy)
    def post_create_table(self, table):
        log.debug("-->")
        '''Adds the `distribute on` clause to create table expressions'''
        if hasattr(table, 'distribute_on'):
            clause = ' DISTRIBUTE ON {columns}'
            if table.distribute_on.column_names[0].lower() == 'random':
                columns = 'RANDOM'
            else: 
                column_list = ','.join(table.distribute_on.column_names)
                columns = '({})'.format(column_list)
            return clause.format(columns=columns)
        else:
            return ''

class NetezzaTypeCompiler(compiler.GenericTypeCompiler):

    def visit_BOOL(self, type_, **kw):
        log.debug("-->")
        return "BOOL"

    def visit_BIGINT(self, type_, **kw):
        log.debug("-->")
        return "BIGINT"

    def visit_INT(self, type_, **kw):
        log.debug("-->")
        return "INT"

    def visit_INT1(self, type_, **kw):
        log.debug("-->")
        return "INT1"

    def visit_INT2(self, type_, **kw):
        log.debug("-->")
        return "INT2"

    def visit_INT4(self, type_, **kw):
        log.debug("-->")
        return "INT4"

    def visit_INT8(self, type_, **kw):
        log.debug("-->")
        return "INT8"

    def visit_BYTEINT(self, type_, **kw):
        log.debug("-->")
        return "BYTEINT"

    def visit_BPCHAR(self, type_, **kw):
        log.debug("-->")
        return "BPCHAR"

    def visit_NCHAR(self, type_, **kw):
        log.debug("-->")
        if type_.length == 1:
            return "NCHAR"
        else:
            return "NCHAR(%d)" % type_.length

    def visit_FLOAT(self, type_, **kw):
        log.debug("-->")
        if not type_.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {"precision": type_.precision}

    def visit_FLOAT4(self, type_, **kw):
        log.debug("-->")
        return "FLOAT4"

    def visit_FLOAT8(self, type_, **kw):
        log.debug("-->")
        return "FLOAT8"

    def visit_DOUBLE_PRECISION(self, type_, **kw):
        log.debug("-->")
        return "DOUBLE PRECISION"

    def visit_TIMESTAMP(self, type_, **kw):
        log.debug("-->")
        return "TIMESTAMP"

    def visit_datetime(self, type_, **kw):
        log.debug("-->")
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_TIME(self, type_, **kw):
        log.debug("-->")
        log.debug(type_.timezone)
        return "TIME%s %s" % (
            "(%d)" % type_.precision
            if getattr(type_, "precision", None) is not None
            else "",
            "WITH TIME ZONE" 
            if type_.timezone 
            else ""
        )

    def visit_TIMETZ(self, type_, **kw):
        log.debug("-->")
        return "TIMETZ"

    def visit_ST_GEOMETRY(self, type_, **kw):
        log.debug("-->")
        if type_.length == 1:
            return "ST_GEOMETRY"
        else:
            return "ST_GEOMETRY(%d)" % type_.length

    def visit_INTERVAL(self, type_, **kw):
        log.debug("-->")
        return "INTERVAL"

    #System Specific
    def visit_OID(self, type_, **kw):
        log.debug("-->")
        return "OID"

    def visit_NAME(self, type_, **kw):
        log.debug("-->")
        return "NAME"

    def visit_BYTEA(self, type_, **kw):
        log.debug("-->")
        return "BYTEA"

    def visit_ABSTIME(self, type_, **kw):
        log.debug("-->")
        return "ABSTIME"

    def visit_unicode(self, type_, **kw):
        return "NVARCHAR(%d)" % type_.length

   #Check usage of following types
    def visit_HSTORE(self, type_, **kw):
        log.debug("-->")
        return "HSTORE"

    def visit_JSON(self, type_, **kw):
        log.debug("-->")
        return "JSON"

    def visit_JSONB(self, type_, **kw):
        log.debug("-->")
        return "JSONB"

    def visit_large_binary(self, type_, **kw):
        log.debug("-->")
        return self.visit_BYTEA(type_, **kw)

class NetezzaIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

class NetezzaInspector(reflection.Inspector):
    def __init__(self, conn):
        log.debug("-->")
        reflection.Inspector.__init__(self, conn)

    #referenced #checkMore
    def get_table_oid(self, table_name, schema=None):
        log.debug("-->")
        """Return the OID for the given table name."""

        return self.dialect.get_table_oid(
            self.bind, table_name, schema, info_cache=self.info_cache
        )

    def get_foreign_table_names(self, schema=None):
        log.debug("-->")
        """Return a list of FOREIGN TABLE names.

        Behavior is similar to that of :meth:`.Inspector.get_table_names`,
        except that the list is limited to those tables that report a
        ``relkind`` value of ``f``.

        """
        schema = schema or self.default_schema_name
        return self.dialect._get_foreign_table_names(self.bind, schema)

    def get_view_names(self, schema=None, include=("plain", "materialized")):
        log.debug("-->")
        """Return all view names in `schema`.

        :param schema: Optional, retrieve names from a non-default schema.
         For special quoting, use :class:`.quoted_name`.

        :param include: specify which types of views to return.  Passed
         as a string value (for a single type) or a tuple (for any number
         of types).  Defaults to ``('plain', 'materialized')``.

        """

        return self.dialect.get_view_names(
            self.bind, schema, info_cache=self.info_cache, include=include
        )

class NetezzaExecutionContext(default.DefaultExecutionContext):
    #referenced
    def fire_sequence(self, seq, type_):
        log.debug("-->")
        return self._execute_scalar(
            (
                "select next value for %s"
                % self.dialect.identifier_preparer.format_sequence(seq)
            ),
            type_,
        )

    #referenced
    def should_autocommit_text(self, statement):
        log.debug("-->")
        return AUTOCOMMIT_REGEXP.match(statement)

# Maps type ids to sqlalchemy types, plus whether they have variable precision
oid_datatype_map = {
    16: (sqltypes.Boolean, False),
    18: (sqltypes.CHAR, False),
    19: (NAME, False),
    20: (sqltypes.BigInteger, False),
    21: (sqltypes.SmallInteger, False),
    23: (sqltypes.Integer, False),
    25: (sqltypes.TEXT, False),
    26: (OID, False),
    700: (sqltypes.REAL, False),
    701: (DOUBLE_PRECISION, False),
    1042: (sqltypes.CHAR, True),
    1043: (sqltypes.String, True),
    1082: (sqltypes.Date, False),
    1083: (TIME, False),
    1184: (TIMESTAMP, False),
    1186: (INTERVAL, False),
    1266: (TIMESTAMP, False),
    1700: (sqltypes.Numeric, False),
    2500: (BYTEINT, False),
    2522: (sqltypes.NCHAR, True),
    2530: (sqltypes.NVARCHAR, True),
    2552: (ST_GEOMETRY, True),
    2568: (sqltypes.VARBINARY, True),
}

class NetezzaDialect(default.DefaultDialect):
    
    name = 'nzdbapi'
    driver = 'netezza'
    dbapi = ""

    supports_alter = True
    max_identifier_length = 63
    supports_sane_rowcount = True

    supports_native_boolean = True
    supports_smallserial = True

    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = True
    postfetch_lastrowid = False

    supports_comments = True
    supports_default_values = True
    supports_empty_insert = False
    supports_multivalues_insert = True
    default_paramstyle = "qmark"
    ischema_names = ischema_names
    requires_name_normalize = True

    colspecs = colspecs
    system_case = "UPPERCASE" # Default case considered

    statement_compiler = NetezzaCompiler
    ddl_compiler = NetezzaDDLCompiler
    type_compiler = NetezzaTypeCompiler
    preparer = NetezzaIdentifierPreparer
    execution_ctx_cls = NetezzaExecutionContext
    inspector = NetezzaInspector
    isolation_level = 'READ COMMITTED'
    encoding = 'utf-8'
    log.debug("isolation_level : " + isolation_level) 
    construct_arguments = [
        (
            schema.Table,
            {
                "ignore_search_path": False,
                "tablespace": None,
                "partition_by": None,
                "with_oids": None,
                "on_commit": None,
                "inherits": None,
            },
        ),
    ]

    reflection_options = ("postgresql_ignore_search_path",)

    _backslash_escapes = True

    @classmethod
    def dbapi(cls):
        log.debug("-->")
        import nzalchemy.nzdbapi as module
        return module

    def __init__(
        self,
        isolation_level=None,
        json_serializer=None,
        json_deserializer=None,
        **kwargs
    ):
        log.debug("-->")
        default.DefaultDialect.__init__(self, **kwargs)
        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer

    def initialize(self, connection):
        log.debug("-->")
        super(NetezzaDialect, self).initialize(connection)
        # PyODBC connector tries to set these to true...
        self.supports_unicode_statements = True
        self.supports_unicode_binds = True
        self.returns_unicode_strings = True
        self.convert_unicode = 'ignore'
        self.ischema_names.update(ischema_names)
        self.system_case = self.get_system_case(connection)
        log.debug (self.system_case)
        log.debug (self._get_current_schema_name(connection))

    def on_connect(self):
        log.debug("-->")
        log.debug(self.isolation_level)
        if self.isolation_level is not None:

            def connect(conn):
                log.debug("-->")
                self.set_isolation_level(conn, self.isolation_level)
                #conn.setencoding(encoding='utf-8')
            return connect
        else:
            return None

    _isolation_lookup = set(
        [
            "AUTOCOMMIT",
            "SERIALIZABLE",
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "REPEATABLE READ",
        ]
    )

    def get_system_case(self, connection):
        log.debug("-->")
        cursor = connection.execute(sql.text('select identifier_case'))
        resultList = cursor.first()
        cursor.close()
        return resultList[0]  

    #referenced #queryError
    #QueryChange
    def set_isolation_level(self, connection, level):
        log.debug("-->")
        level = level.replace("_", " ")
        if level not in self._isolation_lookup:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s"
                % (level, self.name, ", ".join(self._isolation_lookup))
            )
        cursor = connection.cursor()
        #cursor.execute(sql.text("SET TRANSACTION ISOLATION LEVEL %s" % level))
        #cursor.execute("COMMIT")
        cursor.close()

    #referenced
    def get_isolation_level(self, connection):
        log.debug("-->")
        return self.isolation_level

    #referenced
    def _get_default_schema_name(self, connection):
        log.debug("-->")
        return connection.scalar("select current_schema")

    def _get_current_schema_name(self, connection):
        log.debug("-->")
        return connection.scalar("select current_schema")

    def is_system_in_lowercase(self):
        log.debug("-->")
        return True if self.system_case == "lowercase" else False

    #referenced
    def has_table(self, connection, table_name, schema=None):
        log.debug("-->")
        if schema is None:
            schema = self._get_current_schema_name(connection)
        log.debug("has_table_details : " + schema + "-" + table_name)
        if not self.is_system_in_lowercase(): 
            table_name=self.denormalize_name(table_name)
            schema=self.denormalize_name(schema)
        log.debug("has_table_details after denormalize : " + schema + "-" + table_name)
        cursor = connection.execute(
            sql.text( 'select count(*) from _v_table where objid > 200000 and tablename = :name and schema = :schema'
            ).bindparams(
                sql.bindparam( "name", util.text_type(table_name), type_ = sqltypes.Unicode ),
                sql.bindparam( "schema", util.text_type(schema), type_ = sqltypes.Unicode )
                )
        )
        resultList = cursor.first()
        val = bool(resultList[0])
        log.debug(resultList)
        log.debug(val)
        if val:
            log.info("Already Exist"  )
        return val 

    #referenced
    def has_sequence(self, connection, sequence_name, schema=None):
        log.debug("-->")
        if schema is None:
            schema = self._get_current_schema_name(connection)
        if not self.is_system_in_lowercase():
            sequence_name=self.denormalize_name(sequence_name)
            schema=self.denormalize_name(schema)

        cursor = connection.execute(
            sql.text( 'select count(*) from _v_sequence where objid > 200000 and seqname = :name and schema = :schema'
            ).bindparams(
                sql.bindparam( "name", util.text_type(sequence_name), type_ = sqltypes.Unicode ),
                sql.bindparam( "schema", util.text_type(schema), type_ = sqltypes.Unicode )
                )
        )
        resultList = cursor.first()
        val = bool(resultList[0])
        log.debug(resultList)
        log.debug(val)
        if val:
            log.info("Already Exist"  )
        return val 

    #QueryChange
    def _get_server_version_info(self, connection):
        log.debug("-->")
        v = connection.execute("select version()").scalar()
        m = re.match(
            r".*(?:Release) "
            r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
            v,
        )
        if not m:
            raise AssertionError(
                "Could not determine version from string '%s'" % v
            )
        return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        log.debug("-->")
        """Fetch the oid for table_name.

        Several reflection methods require the table oid.  The idea for using
        this method is that it can be fetched one time and cached for
        subsequent calls.
        """
        table_oid = None
        if schema is None:
            schema = self._get_current_schema_name(connection)
        if not self.is_system_in_lowercase():
            table_name=self.denormalize_name(table_name)
            schema=self.denormalize_name(schema)

        cursor = connection.execute(
            sql.text( 'select objid from _v_table where objid > 200000 and tablename = :name and schema = :schema'
            ).bindparams(
                sql.bindparam( "name", util.text_type(table_name), type_ = sqltypes.Unicode ),
                sql.bindparam( "schema", util.text_type(schema), type_ = sqltypes.Unicode )
                )
        )        
        table_oid = cursor.scalar()
        if table_oid is None:
            cursor = connection.execute(
                sql.text( 'select objid from _v_object_data where objid > 200000 and objname = :name and schema = :schema'
                ).bindparams(
                    sql.bindparam( "name", util.text_type(table_name), type_ = sqltypes.Unicode ),
                    sql.bindparam( "schema", util.text_type(schema), type_ = sqltypes.Unicode )
                )
            )
            table_oid = cursor.scalar()
        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        log.debug("-->")
        result = connection.execute(
                "SELECT schema FROM _v_schema "
                "WHERE schemaid > 200000 "
                "ORDER BY schema")
        schema_name = [name[0] for name in result]
        return schema_name

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        log.debug("-->")
        if schema is None:
            schema = self._get_current_schema_name(connection)
        if not self.is_system_in_lowercase():
            schema=self.denormalize_name(schema)

        result = connection.execute(
            sql.text( 'select tablename as name from _v_table where objid > 200000 and schema = :schema'
            ).bindparams(
                sql.bindparam( "schema", util.text_type(schema), type_ = sqltypes.Unicode )
                )
        )
        table_names = [r[0] for r in result]
        return table_names

    @reflection.cache
    def _get_foreign_table_names(self, connection, schema=None, **kw):
        log.debug("-->")
        if schema is None:
            schema = self._get_current_schema_name(connection)
        if not self.is_system_in_lowercase():
            schema=self.denormalize_name(schema)

        result = connection.execute(
            sql.text( "select tablename as name from _v_table where objid > 200000 and schema = :schema and relkind = 'f' "
            ).bindparams(
                sql.bindparam( "schema", util.text_type(schema), type_ = sqltypes.Unicode )
                )
        )
        table_names = [r[0] for r in result]
        return table_names

    @reflection.cache
    def get_temp_table_names(self, connection, **kw):
        literal = 'TEMP TABLE'
        if self.is_system_in_lowercase():
            literal = 'temp table'
        result = connection.execute(
            sql.text( "select tablename as name from _v_table where objid > 200000 and objtype = :literal "
            ).bindparams(
                sql.bindparam( "literal", util.text_type(literal), type_ = sqltypes.Unicode )
                )
        )
        temp_table = [row[0] for row in result]
        return temp_table

    @reflection.cache
    def get_view_names(
        self, connection, schema=None, include=("plain", "materialized"), **kw
    ):
        log.debug("-->")
        if schema is None:
            schema = self._get_current_schema_name(connection)
        if not self.is_system_in_lowercase():
            schema=self.denormalize_name(schema)

        result = connection.execute(
            sql.text(
            "select viewname as name from _v_view where objid > 200000 and schema = :schema" 
            ).columns(viewname=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
        )
        view_name = [r[0] for r in result]
        return view_name

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        log.debug("-->")
        if schema is None:
            schema = self._get_current_schema_name(connection)

        if not self.is_system_in_lowercase():
            view_name=self.denormalize_name(view_name)
            schema=self.denormalize_name(schema)


        definition = connection.scalar(
            sql.text(
                "select definition from _v_view where viewname = :view_name and schema = :schema"
            ).columns(definition=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
            view_name=view_name,
        )
        return definition

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        log.debug("-->")
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        s = sql.text("SELECT CAST(a.attname AS VARCHAR(128)) as name, a.atttypid as typeid, "
                "coldefault as defaultval,not a.attnotnull as nullable, a.attcolleng as length, a.format_type, "
                "description FROM _v_relation_column a WHERE a.objid = :table_oid ORDER BY a.attnum ").columns(table_oid=sqltypes.Unicode)
                
        c = connection.execute(s, table_oid=table_oid)
        rows = c.fetchall()
        # format columns
        columns = []
        for name, typeid, defaultval, nullable, length, format_type,description in rows:
            coltype_class, has_length = oid_datatype_map[typeid]
            if coltype_class is sqltypes.Numeric:
                if self.is_system_in_lowercase():               
                    precision, scale = re.match(r'numeric\((\d+),(\d+)\)', format_type).groups()
                else:
                    precision, scale = re.match(r'NUMERIC\((\d+),(\d+)\)', format_type).groups()

                coltype = coltype_class(int(precision), int(scale))
            elif has_length:
                coltype = coltype_class(length)
            else:
                coltype = coltype_class()
            columns.append({
                'name': name,
                'type': coltype,
                'nullable': nullable,
                'default': defaultval,
                'comment': description,
            })
        return columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        log.debug("-->")
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        PK_SQL = "select attname from _v_relation_keydata where objid = :table_oid and contype = 'p'"
        t = sql.text(PK_SQL).columns(attname=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)
        cols = [r[0] for r in c.fetchall()]
        PK_CONS_SQL = "select constraintname from _v_relation_keydata where objid = :table_oid and contype = 'p'"
        t = sql.text(PK_CONS_SQL).columns(constraintname=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)
        name = c.scalar()
        
        return {"constrained_columns": cols, "name": name}        

    @reflection.cache
    def get_foreign_keys(
        self,
        connection,
        table_name,
        schema=None,
        postgresql_ignore_search_path=False,
        **kw
    ):
        log.debug("-->")
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        if schema is not None:
            if not self.is_system_in_lowercase():
                schema=self.denormalize_name(schema)


        FK_SQL = "select constraintname, attname,pkschema,pkrelation,pkattname from _v_relation_keydata where objid = :table_oid and contype = 'f' and schema = :schemaname"     
        c = connection.execute(
            sql.text(FK_SQL
            ).columns(table_oid=sqltypes.Unicode), 
            table_oid=table_oid,
            schemaname=schema if schema is not None else self.default_schema_name,
        )        
        fkeys = []
        for conname, attname, pkschema, pkrelation , pkattname in c.fetchall():
            if schema is not None and (schema == pkschema or schema == pkschema.lower()):
                # If the actual schema matches the schema of the table
                # we're reflecting, then we will use that.
                pkschema = pkschema
            else :
                pkschema = None
            pkattname = [pkattname]  
            attname = [attname]                 
            fkey_d = {
                "name": conname,
                "constrained_columns": attname,
                "referred_schema": pkschema,
                "referred_table": pkrelation,
                "referred_columns": pkattname,
            }
            fkeys.append(fkey_d)
        return fkeys

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        log.debug("-->")
        '''Netezza doesn't have indexes'''
        return []

    @reflection.cache
    def get_unique_constraints(
        self, connection, table_name, schema=None, **kw
    ):
        log.debug("-->")
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        UNIQUE_SQL = "select constraintname as name, attname as col_name, conseq as col_num from _v_relation_keydata where objid = :table_oid and contype = 'u' order by conseq ASC"
        t = sql.text(UNIQUE_SQL).columns(col_name=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)
        uniques = defaultdict(lambda: defaultdict(dict))
        for name, col_name, col_num in c.fetchall():
            name = name
            uc = uniques[name]
            uc["cols"][col_num] = col_name
        return [            
            {"name": name, "column_names": [uc["cols"][i] for i in uc["cols"]]}
            for name, uc in uniques.items()
        ]

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        log.debug("-->")
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        COMMENT_SQL = "select description from _t_description where objoid = :table_oid"
        c = connection.execute(sql.text(COMMENT_SQL), table_oid=table_oid)        
        return {"text": c.scalar()}

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        log.debug("-->")
        return []

'''
Query should be in format

import nzalchemy as nz

sel = "select * from testdt"
CreateTableAs('testCTA', sel, False, "NZ_DATE, NZ_DATETIME", 'NZ_DATE, NZ_DATETIME')
It will be constructed as
 CREATE  TABLE testCTA AS (
        select * from testdt
           ) DISTRIBUTE ON (NZ_DATE,NZ_DATETIME)
    ORGANIZE ON (NZ_DATE, NZ_DATETIME)
'''
class CreateTableAs(DDLElement):
    """Create a CREATE TABLE AS SELECT ... statement."""
    def __init__(self,
                 new_table_name,
                 select_query,
                 temporary = False,
                 distribute_on = 'random',
                 organize_on=''):
        log.debug("-->") 
        super(CreateTableAs, self).__init__()
        self.select_query = select_query
        self.temporary = temporary
        self.new_table_name = new_table_name
        self.distribute_on = distribute_on
        self.organize_on = organize_on

    def distribute_clause(self):
        log.debug("-->") 
        if self.distribute_on.lower() != 'random':
            column_list = ''.join(self.distribute_on.split())
            return '({})'.format(column_list)
        else:
            log.debug("Random") 
            return 'RANDOM'

    def organize_clause(self):
        if self.organize_on:
            column_list = ''.join(self.organize_on.split())
            return 'ORGANIZE ON ({})'.format(column_list)
        else:
            return '' 

@compiles(CreateTableAs)
def visit_create_table_as(element, compiler, **_kwargs):
    log.debug("-->") 
    '''compiles a ctas statement'''
    createStmt = """
        CREATE {tmp}TABLE {name} AS (
        {select}
        ) DISTRIBUTE ON {distribute}
    """.format(
        tmp = 'TEMP ' if element.temporary else '',
        name = element.new_table_name,
        select = element.select_query,
        distribute = element.distribute_clause(),
    )
    return createStmt + element.organize_clause() 

