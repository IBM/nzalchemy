from . import processors
from .base import NetezzaExecutionContext, NetezzaDialect, log
from sqlalchemy import types as sqltypes, util
import decimal
from .base import DATETIME

class _netezzaNumeric_nzpy(sqltypes.Numeric):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype == 701:
                return processors.to_decimal_processor_factory( decimal.Decimal, self._effective_decimal_return_scale )
            elif coltype == 1700:
                return decimal.Decimal
            else:
                return 
        else:
            return processors.to_float

class _netezzaTimestamp_nzpy(sqltypes.TIMESTAMP):
    def result_processor(self, dialect, coltype):
        return processors.str_to_datetime

class _netezzaDateTime_nzpy(sqltypes.DateTime):
    def result_processor(self, dialect, coltype):
        return processors.str_to_datetime

class _netezzaDate_nzpy(sqltypes.Date):
    def result_processor(self, dialect, coltype):
        return processors.str_to_date

class _netezzaTime_nzpy(sqltypes.Time):
    def result_processor(self, dialect, coltype):
        return processors.str_to_time

class NetezzaExecutionContext_nzpy(NetezzaExecutionContext):
    pass

class _netezzaString_nzpy(sqltypes.String):
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, bytes):
                return value.decode('utf-8')
            return str(value)
        return process

class NetezzaDialect_nzpy(NetezzaDialect):
    supports_statement_cache = True
    execution_ctx_cls = NetezzaExecutionContext_nzpy
    colspecs = util.update_copy(
        NetezzaDialect.colspecs,
        {
            sqltypes.Numeric: _netezzaNumeric_nzpy,
            sqltypes.TIMESTAMP: _netezzaTimestamp_nzpy,
            sqltypes.Date: _netezzaDate_nzpy, 
            sqltypes.Time: _netezzaTime_nzpy,
            sqltypes.DateTime: _netezzaDateTime_nzpy,
            sqltypes.String: _netezzaString_nzpy,
            sqltypes.CHAR: _netezzaString_nzpy,
            sqltypes.VARCHAR: _netezzaString_nzpy
        }
    )
    @classmethod
    def import_dbapi(cls):
        return __import__("nzpy")

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        opts.update(url.query)
        return ([], opts)

    def do_begin(self,connection):
        if hasattr(connection, "connection"):
            connection = connection.connection
            connection.autocommit = False
    
    def set_isolation_level(self, connection, level):
        level = level.replace("_", " ")
        # adjust for ConnectionFairy possibly being present
        if hasattr(connection, "connection"):
            connection = connection.connection

        if level == "AUTOCOMMIT":
            connection.autocommit = True
        else:
            cursor = connection.cursor()
            cursor.execute("BEGIN")
            cursor.close

dialect = NetezzaDialect_nzpy
