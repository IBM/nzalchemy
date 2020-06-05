from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    @property
    def autoincrement_insert(self):
        """target platform generates new surrogate integer primary key values
        when insert() is executed, excluding the pk column."""
        return exclusions.open()
        
    @property
    def index_reflection(self):
        "Netezza does not support index"
        return exclusions.closed()    

    @property
    def temp_table_reflection(self):
        return exclusions.open()
        
    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""
        return exclusions.open()    

    # closed due to sqlalchemy.exc.CompileError: This SELECT structure does
    # not use a simple integer value for limit

    @property
    def bound_limit_offset(self):
        return exclusions.closed()

    @property
    def implicitly_named_constraints(self):
        return exclusions.open()        
        
    @property    
    def reflects_pk_names(self):
        return exclusions.open()         
        
    @property        
    def check_constraint_reflection(self):
        """
        CREATE TABLE sa_cc (
        a INTEGER,
        CONSTRAINT cc1 CHECK (a > 1 AND a < 5),
        CONSTRAINT cc2 CHECK (a = 1 OR (a > 2 AND a < 5))
        )
        
        NOTICE:  check constraints not supported
        NOTICE:  check constraints not supported
        CREATE TABLE

        """
        return exclusions.closed()
        
    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""
        return exclusions.closed()        
        
    @property
    def duplicate_key_raises_integrity_error(self):
        """target dialect raises IntegrityError when reporting an INSERT
        with a primary key violation.  (hint: it should)

        """
        return exclusions.closed()        
        
    @property
    def implicit_decimal_binds(self):
        """target backend will return a selected Decimal as a Decimal, not
        a string.

        e.g.::

            expr = decimal.Decimal("15.7563")

            value = e.scalar(
                select([literal(expr)])
            )

            assert value == expr

        See :ticket:`4036`

        """

        return exclusions.closed()        
      
    @property
    def text_type(self):
        """Netezza database does not support an unbounded Text() "
        "type such as TEXT or CLOB"""

        return exclusions.closed()        
        
    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()        
        
    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.open()        
        
    @property
    def comment_reflection(self):
        return exclusions.open()                
        
    @property
    def datetime_historic(self):
        """Netezza dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return exclusions.open()

    @property
    def date_historic(self):
        """Netezza dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return exclusions.open()        
        
    @property
    def timestamp_microseconds(self):
        """Netezza dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""
        return exclusions.open()        
        
    @property
    def time_microseconds(self):
        """Netezza dialect does not supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()         
        
    @property
    def percent_schema_names(self):
        """target backend supports weird identifiers with percent signs
        in them, e.g. 'some % column'.

        this is a very weird use case but often has problems because of
        DBAPIs that use python formatting.  It's not a critical use
        case either.

        """
        return exclusions.open()        
        
    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""
        return exclusions.open()        
        
    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.open()        
        
    @property
    def autocommit(self):
        """target dialect supports 'AUTOCOMMIT' as an isolation_level"""
        return exclusions.open()
        
    def get_isolation_levels(self, config):
        
        return {
            "default": "READ COMMITTED",
            "supported": [
                "SERIALIZABLE", "READ UNCOMMITTED",
                "READ COMMITTED", "REPEATABLE READ",
                "AUTOCOMMIT"
            ]
        }        
    
    # Below test cases are closed in testing.suite.requirements.py as well:
    
    @property
    def dbapi_lastrowid(self):
        """"pyodbc does not includes a 'lastrowid' accessor on the DBAPI
        cursor object.
        Error: 'pyodbc.Cursor' object has no attribute 'lastrowid'
        """
        return exclusions.closed()
        
    @property
    def ctes(self):
        """Netezza database supports CTEs
        Below query fails as : 
        
        => WITH some_cte AS (SELECT some_table.id AS id, some_table.data AS data, some_table.parent_id AS parent_id FROM some_table WHERE some_table.data IN ('d2', 'd3', 'd4')) INSERT INTO some_other_table (id, data, parent_id) SELECT some_cte.id, some_cte.data, some_cte.parent_id FROM some_cte;
        
        Error: 2020-04-29 22:39:30.068287 PDT [17827]  ERROR:  'WITH some_cte AS (SELECT some_table.id AS id, some_table.data AS data, some_table.parent_id AS parent_id FROM some_table WHERE some_table.data IN ('d2', 'd3', 'd4')) INSERT INTO some_other_table (id, data, parent_id) SELECT some_cte.id, some_cte.data, some_cte.parent_id FROM some_cte' 
        error  ^ found "INSERT" (at char 167) expecting `SELECT' or `'(''
        
        Similar error for UPDATE and DELETE
        
        """

        return exclusions.closed()
        
    @property
    def computed_columns(self):
        """
        Netezza does not supports computed columns. Fails with below error: 
        
        CREATE TABLE square (
        id INTEGER NOT NULL,
        side INTEGER,
        area INTEGER GENERATED ALWAYS AS (side * side),
        perimeter INTEGER GENERATED ALWAYS AS (4 * side),
        PRIMARY KEY (id)
        )   
        
        error   ^ found "GENERATED" (at char 95) expecting next item or end of list
        """
        return exclusions.closed()        

    @property
    def computed_columns_stored(self):
        "Netezza does not supports computed columns with `persisted=True`"
        return exclusions.closed()

    @property
    def computed_columns_virtual(self):
        "Netezza does not supports computed columns with `persisted=False`"
        return exclusions.closed()

    @property
    def computed_columns_default_persisted(self):
        """If the default persistence is virtual or stored when `persisted`
        is omitted"""
        return exclusions.closed()

    @property
    def computed_columns_reflect_persisted(self):
        """If persistence information is returned by the reflection of
        computed columns"""
        return exclusions.closed()
        
    @property
    def tuple_in(self):
        """Netezza platform does not supports the syntax
        "(x, y) IN ((x1, y1), (x2, y2), ...)"
        => SELECT some_table.id FROM some_table WHERE (some_table.x, some_table.z) IN ((1, 'z1'), (2, 'z2')) ORDER BY some_table.id;
        ERROR:  'SELECT some_table.id FROM some_table WHERE (some_table.x, some_table.z) IN ((1, 'z1'), (2, 'z2')) ORDER BY some_table.id;'
        error 
        """

        return exclusions.closed()      
        
    @property
    def temporary_views(self):
        """Netezza database does not supports temporary views"""
        return exclusions.closed()        

    @property
    def foreign_key_constraint_option_reflection_ondelete(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_option_reflection_onupdate(self):
        return exclusions.closed()                
        
    @property
    def datetime_literals(self):
        """target dialect does not supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.
        """
        return exclusions.closed()       

    @property
    def denormalized_names(self):
        """Netezza system can be in UPPERCASE or LOWERCASE"""
        return exclusions.closed() 
 
