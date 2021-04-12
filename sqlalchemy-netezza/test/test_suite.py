import pytest
import operator

import sqlalchemy as sa
from sqlalchemy.testing.suite import *
from sqlalchemy import literal_column
from sqlalchemy import literal
from sqlalchemy import inspect
from sqlalchemy.testing.provision import temp_table_keyword_args
from sqlalchemy import inspect
from sqlalchemy import testing
from sqlalchemy.testing import eq_
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from sqlalchemy import types as sql_types
from sqlalchemy.schema import Column
from sqlalchemy import Integer

@temp_table_keyword_args.for_db("netezza")

def _netezza_temp_table_keyword_args(cfg, eng):

    return {"prefixes": ["TEMPORARY"]}
    
from sqlalchemy.testing.suite import (
    DateTest as _DateTest,
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
    DateTimeTest as _DateTimeTest, 
    ExpandingBoundInTest as _ExpandingBoundInTest,
    NumericTest as _NumericTest,
    IsOrIsNotDistinctFromTest as _IsOrIsNotDistinctFromTest,
    InsertBehaviorTest as _InsertBehaviorTest,
    LastrowidTest as _LastrowidTest,
    TimeTest as _TimeTest,
    UnicodeVarcharTest as _UnicodeVarcharTest,
    TableDDLTest as _TableDDLTest,
    IntegerTest as _IntegerTest,
    ComponentReflectionTest as _ComponentReflectionTest,
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
    DateHistoricTest as _DateHistoricTest,
    DateTimeHistoricTest as _DateTimeHistoricTest,
    TimestampMicrosecondsTest as _TimestampMicrosecondsTest,
    QuotedNameArgumentTest as _QuotedNameArgumentTest,
)

class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    @pytest.mark.skip()
    def quote_fixtures(cls):
        return

    @pytest.mark.skip()
    def quote_fixtures(cls):
        return

    @pytest.mark.skip()
    def test_get_table_options(cls):
        return

    @pytest.mark.skip()
    def test_get_view_definition(cls):
        return

    @pytest.mark.skip()
    def test_get_columns(cls):
        return

    @pytest.mark.skip()
    def test_get_pk_constraint(cls):
        return

    @pytest.mark.skip()
    def test_get_foreign_keys(cls):
        return

    @pytest.mark.skip()
    def test_get_indexes(cls):
        return

    @pytest.mark.skip()
    def test_get_unique_constraints(cls):
        return 

    @pytest.mark.skip()
    def test_get_table_comment(cls):
        return

    @pytest.mark.skip()
    def test_get_check_constraints(cls):
        return

class DateHistoricTest(_DateHistoricTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )    
     
class DateTimeHistoricTest(_DateTimeHistoricTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )    

class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):

    @pytest.mark.skip()
    def test_fk_column_order(self):
        return

    @pytest.mark.skip()
    def test_pk_column_order(self):
        return

class ComponentReflectionTest(_ComponentReflectionTest): 

    @classmethod
    def define_temp_tables(cls, metadata):
        kw = temp_table_keyword_args(config, config.db)
        user_tmp = Table(
            "user_tmp",
            metadata,
            Column("id", sa.INT, primary_key=True),
            Column("name", sa.VARCHAR(50)),
            Column("foo", sa.INT),
            sa.UniqueConstraint("name", name="user_tmp_uq"),
            **kw
        )
        if (
            testing.requires.view_reflection.enabled
            and testing.requires.temporary_views.enabled
        ):
            event.listen(
                user_tmp,
                "after_create",
                DDL(
                    "create temporary view user_tmp_v as "
                    "select * from user_tmp"
                ),
            )
            event.listen(user_tmp, "before_drop", DDL("drop view user_tmp_v"))
            
    @pytest.mark.skip()           
    def test_get_temp_table_indexes():
        return
        
    @testing.requires.table_reflection
    @testing.provide_metadata
    def test_autoincrement_col(self):
        """test that 'autoincrement' is reflected according to sqla's policy.

        Don't mark this test as unsupported for any backend !

        (technically it fails with MySQL InnoDB since "id" comes before "id2")

        A backend is better off not returning "autoincrement" at all,
        instead of potentially returning "False" for an auto-incrementing
        primary key column.

        """

        meta = self.metadata
        insp = inspect(meta.bind)

        for tname, cname in [
            ("USERS", "USER_ID"),
            ("EMAIL_ADDRESSES", "ADDRESS_ID"),
            ("DINGALINGS", "DINGALING_ID"),
        ]:
            cols = insp.get_columns(tname)
            id_ = {c["name"]: c for c in cols}[cname]
            assert id_.get("autoincrement", True)
            
    def _test_get_columns(self, schema=None, table_type="table"):
        meta = MetaData(testing.db)
        users, addresses = (self.tables.users, self.tables.email_addresses)
        table_names = ["USERS", "EMAIL_ADDRESSES"]
        if table_type == "view":
            table_names = ["USERS_V", "EMAIL_ADDRESSES_V"]
        insp = inspect(meta.bind)
        for table_name, table in zip(table_names, (users, addresses)):
            schema_name = schema
            cols = insp.get_columns(table_name, schema=schema_name)
            self.assert_(len(cols) > 0, len(cols))

            # should be in order

            for i, col in enumerate(table.columns):
                eq_(col.name, cols[i]["name"].lower())
                ctype = cols[i]["type"].__class__
                ctype_def = col.type
                if isinstance(ctype_def, sa.types.TypeEngine):
                    ctype_def = ctype_def.__class__

                # Oracle returns Date for DateTime.

                if testing.against("oracle") and ctype_def in (
                    sql_types.Date,
                    sql_types.DateTime,
                ):
                    ctype_def = sql_types.Date

                # assert that the desired type and return type share
                # a base within one of the generic types.

                self.assert_(
                    len(
                        set(ctype.__mro__)
                        .intersection(ctype_def.__mro__)
                        .intersection(
                            [
                                sql_types.Integer,
                                sql_types.Numeric,
                                sql_types.DateTime,
                                sql_types.Date,
                                sql_types.Time,
                                sql_types.String,
                                sql_types._Binary,
                            ]
                        )
                    )
                    > 0,
                    "%s(%s), %s(%s)"
                    % (col.name, col.type, cols[i]["name"], ctype),
                )

                if not col.primary_key:
                    assert cols[i]["default"] is None

    @testing.provide_metadata
    def _test_get_table_names(
        self, schema=None, table_type="table", order_by=None
    ):
        _ignore_tables = [
            "COMMENT_TEST",
            "LOCAL_TABLE",
            "REMOTE_TABLE",
            "REMOTE_TABLE_2",
        ]
        meta = self.metadata

        insp = inspect(meta.bind)

        if table_type == "view":
            table_names = insp.get_view_names(schema)
            table_names.sort()
            answer = ["EMAIL_ADDRESSES_V", "USERS_V"]
            eq_(sorted(table_names), answer)
        else:
            if order_by:
                tables = [
                    rec[0]
                    for rec in insp.get_sorted_table_and_fkc_names(schema)
                    if rec[0]
                ]
            else:
                tables = insp.get_table_names(schema)
            table_names = [t for t in tables if t not in _ignore_tables]

            if order_by == "foreign_key":
                answer = ["USERS", "EMAIL_ADDRESSES", "DINGALINGS"]
                eq_(table_names, answer)
            else:
                answer = ["DINGALINGS", "EMAIL_ADDRESSES", "USERS"]
                eq_(sorted(table_names), answer)

    @testing.requires.temp_table_names
    def test_get_temp_table_names(self):
        insp = inspect(self.bind)
        temp_table_names = insp.get_temp_table_names()
        eq_(sorted(temp_table_names), ["USER_TMP"])
        
    @testing.requires.temp_table_reflection
    def test_get_temp_table_columns(self):
        meta = MetaData(self.bind)
        user_tmp = self.tables.user_tmp
        insp = inspect(meta.bind)
        cols = insp.get_columns("user_tmp")
        self.assert_(len(cols) > 0, len(cols))

        for i, col in enumerate(user_tmp.columns):
            eq_(col.name, cols[i]["name"].lower())        

    def _test_get_comments(self, schema=None):
        insp = inspect(testing.db)

        eq_(
            insp.get_table_comment("comment_test", schema=schema),
            {"text": r"""the test % ' " \ table comment"""},
        )

        eq_(insp.get_table_comment("users", schema=schema), {"text": None})

        eq_(
            [
                {"name": rec["name"], "comment": rec["comment"]}
                for rec in insp.get_columns("comment_test", schema=schema)
            ],
            [
                {"comment": "id comment", "name": "ID"},
                {"comment": "data % comment", "name": "DATA"},
                {
                    "comment": (
                        r"""Comment types type speedily ' " \ '' Fun!"""
                    ),
                    "name": "D2",
                },
            ],
        )
        
    @testing.requires.table_reflection
    @testing.provide_metadata
    def test_nullable_reflection(self):
        t = Table(
            "t",
            self.metadata,
            Column("a", Integer, nullable=True),
            Column("b", Integer, nullable=False),
        )
        t.create()
        eq_(
            dict(
                (col["name"], col["nullable"])
                for col in inspect(self.metadata.bind).get_columns("t")
            ),
            {"A": True, "B": False},
        )        

    @testing.provide_metadata
    def _test_get_pk_constraint(self, schema=None):
        meta = self.metadata
        users, addresses = self.tables.users, self.tables.email_addresses
        insp = inspect(meta.bind)

        users_cons = insp.get_pk_constraint(users.name, schema=schema)
        users_pkeys = users_cons["constrained_columns"]
        eq_(users_pkeys, ["USER_ID"])

        addr_cons = insp.get_pk_constraint(addresses.name, schema=schema)
        addr_pkeys = addr_cons["constrained_columns"]
        eq_(addr_pkeys, ["ADDRESS_ID"])

        with testing.requires.reflects_pk_names.fail_if():
            eq_(addr_cons["name"], "EMAIL_AD_PK")

    @testing.provide_metadata
    def _test_get_foreign_keys(self, schema=None):
        meta = self.metadata
        users, addresses = (self.tables.users, self.tables.email_addresses)
        insp = inspect(meta.bind)
        if schema is not None: 
            expected_schema = schema.upper()
        else:
            expected_schema = schema
        # users

        if testing.requires.self_referential_foreign_keys.enabled:
            users_fkeys = insp.get_foreign_keys(users.name, schema=schema)
            fkey1 = users_fkeys[0]

            with testing.requires.named_constraints.fail_if():
                eq_(fkey1["name"], "USER_ID_FK")

            eq_(fkey1["referred_schema"], expected_schema)
            eq_(fkey1["referred_table"], users.name.upper())
            eq_(fkey1["referred_columns"], ["USER_ID"])
            if testing.requires.self_referential_foreign_keys.enabled:
                eq_(fkey1["constrained_columns"], ["PARENT_USER_ID"])

        # addresses
        addr_fkeys = insp.get_foreign_keys(addresses.name, schema=schema)
        fkey1 = addr_fkeys[0]

        with testing.requires.implicitly_named_constraints.fail_if():
            self.assert_(fkey1["name"] is not None)

        eq_(fkey1["referred_schema"], expected_schema)
        eq_(fkey1["referred_table"], users.name.upper())
        eq_(fkey1["referred_columns"], ["USER_ID"])
        eq_(fkey1["constrained_columns"], ["REMOTE_USER_ID"])

    @testing.requires.schema_reflection
    def test_get_schema_names(self):
        insp = inspect(testing.db)

        self.assert_(testing.config.test_schema.upper() in insp.get_schema_names())
    
    @testing.requires.temp_table_reflection
    @testing.requires.unique_constraint_reflection
    def test_get_temp_table_unique_constraints(self):
        insp = inspect(self.bind)
        reflected = insp.get_unique_constraints("user_tmp")
        for refl in reflected:
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            refl.pop("duplicates_index", None)
        eq_(reflected, [{"column_names": ["NAME"], "name": "USER_TMP_UQ"}])


    @testing.provide_metadata
    def _test_get_unique_constraints(self, schema=None):
        # SQLite dialect needs to parse the names of the constraints
        # separately from what it gets from PRAGMA index_list(), and
        # then matches them up.  so same set of column_names in two
        # constraints will confuse it.    Perhaps we should no longer
        # bother with index_list() here since we have the whole
        # CREATE TABLE?
        uniques = sorted(
            [
                {"name": "UNIQUE_A", "column_names": ["A"]},
                {"name": "UNIQUE_A_B_C", "column_names": ["A", "B", "C"]},
                {"name": "UNIQUE_C_B_A", "column_names": ["C", "A", "B"]},
                {"name": "UNIQUE_ASC_KEY", "column_names": ["ASC", "KEY"]},
                {"name": "i.have.dots", "column_names": ["B"]},
                {"name": "i have spaces", "column_names": ["C"]},   
            ],
            key=operator.itemgetter("name"),
        )
        orig_meta = self.metadata
        table = Table(
            "testtbl",
            orig_meta,
            Column("A", sa.String(20)),
            Column("B", sa.String(30)),
            Column("C", sa.Integer),
            # reserved identifiers
            Column("ASC", sa.String(30)),
            Column("KEY", sa.String(30)),
            schema=schema,
        )
        for uc in uniques:
            table.append_constraint(
                sa.UniqueConstraint(*uc["column_names"], name=uc["name"])
            )
        orig_meta.create_all()

        inspector = inspect(orig_meta.bind)
        reflected = sorted(
            inspector.get_unique_constraints("testtbl", schema=schema),
            key=operator.itemgetter("name"),
        )

        names_that_duplicate_index = set()

        for orig, refl in zip(uniques, reflected):
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            dupe = refl.pop("duplicates_index", None)
            if dupe:
                names_that_duplicate_index.add(dupe)
            eq_(orig, refl)

        reflected_metadata = MetaData()
        reflected = Table(
            "testtbl",
            reflected_metadata,
            autoload_with=orig_meta.bind,
            schema=schema,
        )

        # test "deduplicates for index" logic.   MySQL and Oracle
        # "unique constraints" are actually unique indexes (with possible
        # exception of a unique that is a dupe of another one in the case
        # of Oracle).  make sure # they aren't duplicated.
        idx_names = set([idx.name for idx in reflected.indexes])
        uq_names = set(
            [
                uq.name
                for uq in reflected.constraints
                if isinstance(uq, sa.UniqueConstraint)
            ]
        ).difference(["unique_c_a_b"])

        assert not idx_names.intersection(uq_names)
        if names_that_duplicate_index:
            eq_(names_that_duplicate_index, idx_names)
            eq_(uq_names, set())

    @testing.provide_metadata
    def _test_get_view_definition(self, schema=None):
        meta = self.metadata
        view_name1 = "USERS_V"
        view_name2 = "EMAIL_ADDRESSES_V"
        insp = inspect(meta.bind)
        v1 = insp.get_view_definition(view_name1, schema=schema)
        self.assert_(v1)
        v2 = insp.get_view_definition(view_name2, schema=schema)
        self.assert_(v2)

class IntegerTest(_IntegerTest):

    @testing.provide_metadata
    def _round_trip(self, datatype, data):
        metadata = self.metadata
        int_table = Table(
            "integer_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("integer_data", datatype),
        )

        metadata.create_all(config.db)
        config.db.execute(int_table.insert(), {"integer_data": data})
        row = config.db.execute(select([int_table.c.integer_data])).first()
        eq_(row, (data,))

        if util.py3k:
            assert isinstance(row[0], int)
        else:
            assert isinstance(row[0], (long, int))  # noqa
        
class TimeTest(_TimeTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )

class UnicodeVarcharTest(_UnicodeVarcharTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "unicode_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("unicode_data", cls.datatype),
        )
    
    def test_round_trip(self):
        return

    def test_round_trip_executemany(self):
        return  
        
class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )

class DateTimeTest(_DateTimeTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )

class InsertBehaviorTest(_InsertBehaviorTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "autoinc_pk",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("data", String(50)),
        )
        Table(
            "manual_pk",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )
        Table(
            "includes_defaults",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("data", String(50)),
            Column("x", Integer, default=5),
            Column(
                "y",
                Integer,
                default=literal_column("2", type_=Integer) + literal(2),
            ),
        )
        
class LastrowidTest(_LastrowidTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "autoinc_pk",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("data", String(50)),
        )

        Table(
            "manual_pk",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )

class DateTest(_DateTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )
        
class DateTimeCoercedToDateTimeTest(_DateTest): 
       
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )
        
class TimestampMicrosecondsTest(_TimestampMicrosecondsTest):   

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, Sequence('id_seq'), primary_key=True
            ),
            Column("date_data", cls.datatype),
        )     
        
class IsOrIsNotDistinctFromTest (_IsOrIsNotDistinctFromTest): # Failed - Netezza doesn't support IS DISTINCT feature 
    @pytest.mark.skip()        
    def test_is_or_isnot_distinct_from(cls):
        return

    @pytest.mark.skip()        
    def test_is_or_isnot_distinc(cls):
        return
        
class TableDDLTest(_TableDDLTest):
    @pytest.mark.skip()           
    def test_underscore_names():
        return
