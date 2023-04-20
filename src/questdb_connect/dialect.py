#
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2023 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import abc
import enum

import sqlalchemy as sqla
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.compiler import DDLCompiler, GenericTypeCompiler, IdentifierPreparer, SQLCompiler

# https://docs.sqlalchemy.org/en/14/ apache-superset requires SQLAlchemy 1.4


# ===== QUESTDB PARTITION TYPE =====

class PartitionBy(enum.Enum):
    DAY = 0
    MONTH = 1
    YEAR = 2
    NONE = 3
    HOUR = 4
    WEEK = 5


# ===== QUESTDB DATA TYPES =====

class QDBType:
    """Base class for all questdb_connect types"""
    __visit_name__ = 'QuestDBType'

    def column_spec(self, column_name: str):
        return f"'{column_name}' {self.__visit_name__}"


class Boolean(sqla.Boolean, QDBType):
    __visit_name__ = 'BOOLEAN'


class Byte(sqla.Integer, QDBType):
    __visit_name__ = 'BYTE'


class Short(sqla.Integer, QDBType):
    __visit_name__ = 'SHORT'


class Int(sqla.Integer, QDBType):
    __visit_name__ = 'INT'


class Integer(Int):
    pass


class Long(sqla.Integer, QDBType):
    __visit_name__ = 'LONG'


class Float(sqla.Float, QDBType):
    __visit_name__ = 'FLOAT'


class Double(sqla.Float, QDBType):
    __visit_name__ = 'DOUBLE'


class Symbol(sqla.String, QDBType):
    __visit_name__ = 'SYMBOL'


class String(sqla.String, QDBType):
    __visit_name__ = 'STRING'


class Char(sqla.String, QDBType):
    __visit_name__ = 'CHAR'


class Long256(sqla.String, QDBType):
    __visit_name__ = 'LONG256'


class UUID(sqla.String, QDBType):
    __visit_name__ = 'UUID'


class Date(sqla.Date, QDBType):
    __visit_name__ = 'DATE'


class Timestamp(sqla.DateTime, QDBType):
    __visit_name__ = 'TIMESTAMP'


_GEOHASH_MAX_BITS = 60


def geohash_type(bits: int):
    """Factory for Geohash(<bits>b) types"""
    if not isinstance(bits, int) or bits < 0 or bits > _GEOHASH_MAX_BITS:
        raise AttributeError(f'bits should be of type int [0, {_GEOHASH_MAX_BITS}]')

    class GeohashWithPrecision(sqla.String, QDBType):
        __visit_name__ = f'GEOHASH({bits}b)'
        bit_precision = bits

    return GeohashWithPrecision


# ===== SQLAlchemy Dialect ======

def connection_uri(host: str, port: int, username: str, password: str, database: str = 'main'):
    return f'questdb://{username}:{password}@{host}:{port}/{database}'


def create_engine(host: str, port: int, username: str, password: str, database: str = 'main'):
    return sqla.create_engine(connection_uri(host, port, username, password, database))


class QDBEngine(SchemaEventTarget):
    def __init__(
            self,
            ts_col_name: str = None,
            partition_by: PartitionBy = PartitionBy.DAY,
            is_wal: bool = True
    ):
        self.ts_col_name = ts_col_name
        self.partition_by = partition_by
        self.is_wal = is_wal
        self.compiled = None

    def get_table_suffix(self):
        if self.compiled is None:
            self.compiled = ''
            has_ts = self.ts_col_name is not None
            is_partitioned = self.partition_by and self.partition_by != PartitionBy.NONE
            if has_ts:
                self.compiled += f'TIMESTAMP({self.ts_col_name})'
            if is_partitioned:
                if not has_ts:
                    raise ArgumentError(None, 'Designated timestamp must be specified for partitioned table')
                self.compiled += f' PARTITION BY {self.partition_by.name}'
            if self.is_wal:
                if not is_partitioned:
                    raise ArgumentError(None, 'Designated timestamp and partition by must be specified for WAL table')
                if self.is_wal:
                    self.compiled += ' WAL'
                else:
                    self.compiled += ' BYPASS WAL'
        return self.compiled

    def _set_parent(self, parent, **_kwargs):
        parent.engine = self


_QUOTES = ("'", '"')


def _quote_identifier(identifier: str):
    if not identifier:
        return None
    first = 0
    last = len(identifier)
    if identifier[first] in _QUOTES:
        first += 1
    if identifier[last - 1] in _QUOTES:
        last -= 1
    return f"'{identifier[first:last]}'"


class QDBIdentifierPreparer(IdentifierPreparer):
    """QuestDB's identifiers are better off with quotes"""
    quote_identifier = staticmethod(_quote_identifier)

    def _requires_quotes(self, _value):
        return True


class QDBDDLCompiler(DDLCompiler):
    def visit_create_schema(self, create, **kw):
        raise Exception('QuestDB does not support SCHEMAS, there is only "public"')

    def visit_drop_schema(self, drop, **kw):
        raise Exception('QuestDB does not support SCHEMAS, there is only "public"')

    def visit_create_table(self, create, **kw):
        table = create.element
        create_table = f"CREATE TABLE '{table.fullname}' ("
        create_table += ', '.join([self.get_column_specification(c.element) for c in create.columns])
        return create_table + ') ' + table.engine.get_table_suffix()

    def get_column_specification(self, column: sqla.Column, **_):
        if not isinstance(column.type, QDBType):
            raise ArgumentError('Column type is not a valid QuestDB type')
        return column.type.column_spec(column.name)


class QDBSQLCompiler(SQLCompiler):
    def _is_safe_for_fast_insert_values_helper(self):
        return True


class QuestDBDialect(PGDialect_psycopg2, abc.ABC):
    name = 'questdb'
    psycopg2_version = (2, 9)
    default_schema_name = 'public'
    statement_compiler = QDBSQLCompiler
    ddl_compiler = QDBDDLCompiler
    type_compiler = GenericTypeCompiler
    preparer = QDBIdentifierPreparer
    supports_schemas = False
    supports_statement_cache = False
    supports_server_side_cursors = False
    supports_views = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    inline_comments = False
    postfetch_lastrowid = False
    max_identifier_length = 255

    def get_schema_names(self, connection, **kw):
        return ['public']

    def get_table_names(self, connection, schema=None, **kw):
        return [row.table for row in connection.execute(sqla.text('SHOW TABLES'))]

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, postgresql_ignore_search_path=False, **kw):
        return []

    def get_temp_table_names(self, connection, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        pass

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None):
        query = f"tables() WHERE name='{table_name}'"
        result = connection.execute(sqla.text(query))
        return result.rowcount == 1

    def has_sequence(self, connection, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_connection, level):
        pass

    def get_isolation_level(self, dbapi_connection):
        return None
