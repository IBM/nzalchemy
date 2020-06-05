# nzalchemy/pyodbc.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for IBM Netezza via pyodbc.

pyodbc is available at:

    http://pypi.python.org/pypi/pyodbc/

Connecting
^^^^^^^^^^

Examples of pyodbc connection string URLs:

* ``netezza+pyodbc://mydsn`` - connects using the specified DSN named ``mydsn``.

"""

from sqlalchemy import processors
from .base import NetezzaExecutionContext, NetezzaDialect, log
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy import types as sqltypes, util
import decimal
import pyodbc

class _netezzaNumeric_pyodbc(sqltypes.Numeric):
    """Turns Decimals with adjusted() < 0 or > 7 into strings.

    The routines here are needed for older pyodbc versions
    as well as current mxODBC versions.

    """

    def bind_processor(self, dialect):
        log.debug("-->")

        super_process = super(_netezzaNumeric_pyodbc, self).bind_processor(
            dialect
        )

        if not dialect._need_decimal_fix:
            log.debug("dialect._need_decimal_fix")
            return super_process

        def process(value):
            log.debug("-->")
            if self.asdecimal and isinstance(value, decimal.Decimal):

                adjusted = value.adjusted()
                if adjusted < 0:
                    return self._small_dec_to_string(value)
                elif adjusted > 7:
                    return self._large_dec_to_string(value)

            if super_process:
                return super_process(value)
            else:
                return value

        return process
        
    def result_processor(self, dialect, coltype):        
        if self.asdecimal:
            return processors.to_decimal_processor_factory(
                decimal.Decimal, self._effective_decimal_return_scale
            )
        elif dialect.supports_native_decimal:
            return processors.to_float
        else:
            return None        

    # these routines needed for older versions of pyodbc.
    # as of 2.1.8 this logic is integrated.

    def _small_dec_to_string(self, value):
        log.debug("-->")
        return "%s0.%s%s" % (
            (value < 0 and "-" or ""),
            "0" * (abs(value.adjusted()) - 1),
            "".join([str(nint) for nint in value.as_tuple()[1]]),
        )

    def _large_dec_to_string(self, value):
        log.debug("-->")
        _int = value.as_tuple()[1]
        if "E" in str(value):
            result = "%s%s%s" % (
                (value < 0 and "-" or ""),
                "".join([str(s) for s in _int]),
                "0" * (value.adjusted() - (len(_int) - 1)),
            )
        else:
            if (len(_int) - 1) > value.adjusted():
                result = "%s%s.%s" % (
                    (value < 0 and "-" or ""),
                    "".join([str(s) for s in _int][0 : value.adjusted() + 1]),
                    "".join([str(s) for s in _int][value.adjusted() + 1 :]),
                )
            else:
                result = "%s%s" % (
                    (value < 0 and "-" or ""),
                    "".join([str(s) for s in _int][0 : value.adjusted() + 1]),
                )
        return result


class NetezzaExecutionContext_pyodbc(NetezzaExecutionContext):
    pass


class NetezzaDialect_pyodbc(PyODBCConnector, NetezzaDialect):
    execution_ctx_cls = NetezzaExecutionContext_pyodbc
    pyodbc_driver_name = "Netezza pyodbc"
    _need_decimal_fix = True
    
    @classmethod
    def dbapi(cls):
        log.debug("-->")
        return pyodbc

    colspecs = util.update_copy(
        NetezzaDialect.colspecs, {sqltypes.Numeric: _netezzaNumeric_pyodbc}
    )
    def on_connect(self):
        log.debug("-->")
        super_ = super(NetezzaDialect_pyodbc, self).on_connect()

        def on_connect(conn):
            log.debug("-->")
            if super_ is not None:
                super_(conn)
            conn.setencoding(encoding='utf-8')
            conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
            conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
            conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-8')

        return on_connect

dialect = NetezzaDialect_pyodbc

