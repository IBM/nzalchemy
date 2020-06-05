from sqlalchemy.dialects import registry
import pytest

registry.register(
    "netezza.pyodbc", "nzalchemy.pyodbc", "NetezzaDialect_pyodbc"
)

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *
