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
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.compiler import DDLCompiler, GenericTypeCompiler, IdentifierPreparer, SQLCompiler
from sqlalchemy.sql.visitors import Traversible

# https://docs.sqlalchemy.org/en/14/ apache-superset requires SQLAlchemy 1.4


# ===== SQLAlchemy Dialect ======

def connection_uri(host: str, port: int, username: str, password: str, database: str = 'main'):
    return f'questdb://{username}:{password}@{host}:{port}/{database}'


def create_engine(host: str, port: int, username: str, password: str, database: str = 'main'):
    return sqla.create_engine(connection_uri(host, port, username, password, database))


# ===== QUESTDB PARTITION TYPE =====

class PartitionBy(enum.Enum):
    DAY = 0
    MONTH = 1
    YEAR = 2
    NONE = 3
    HOUR = 4
    WEEK = 5


# ===== QUESTDB DATA TYPES =====

class QDBTypeMixin:
    """Base class for all questdb_connect types"""
    __visit_name__ = 'QuestDBType'

    def column_spec(self, column_name: str):
        return f"'{column_name}' {self.__visit_name__}"


class Boolean(sqla.Boolean, QDBTypeMixin):
    __visit_name__ = 'BOOLEAN'


class Byte(sqla.Integer, QDBTypeMixin):
    __visit_name__ = 'BYTE'


class Short(sqla.Integer, QDBTypeMixin):
    __visit_name__ = 'SHORT'


class Int(sqla.Integer, QDBTypeMixin):
    __visit_name__ = 'INT'


class Integer(Int):
    pass


class Long(sqla.Integer, QDBTypeMixin):
    __visit_name__ = 'LONG'


class Float(sqla.Float, QDBTypeMixin):
    __visit_name__ = 'FLOAT'


class Double(sqla.Float, QDBTypeMixin):
    __visit_name__ = 'DOUBLE'


class Symbol(sqla.String, QDBTypeMixin):
    __visit_name__ = 'SYMBOL'


class String(sqla.String, QDBTypeMixin):
    __visit_name__ = 'STRING'


class Char(sqla.String, QDBTypeMixin):
    __visit_name__ = 'CHAR'


class Long256(sqla.String, QDBTypeMixin):
    __visit_name__ = 'LONG256'


class UUID(sqla.String, QDBTypeMixin):
    __visit_name__ = 'UUID'


class Date(sqla.Date, QDBTypeMixin):
    __visit_name__ = 'DATE'


class Timestamp(sqla.DateTime, QDBTypeMixin):
    __visit_name__ = 'TIMESTAMP'


_GEOHASH_MAX_BITS = 60


def geohash_type(bits: int):
    """Factory for Geohash(<bits>b) types"""
    if not isinstance(bits, int) or bits < 0 or bits > _GEOHASH_MAX_BITS:
        raise AttributeError(f'bits should be of type int [0, {_GEOHASH_MAX_BITS}]')

    class GeohashWithPrecision(sqla.String, QDBTypeMixin):
        __visit_name__ = f'GEOHASH({bits}b)'
        bit_precision = bits

    return GeohashWithPrecision


def resolve_type_from_name(type_name):
    if not type_name:
        return None
    print(f'PAZUZU: {type_name}')
    name_u = type_name.upper()
    qdbc_type = None
    if name_u == 'BOOLEAN':
        qdbc_type = Boolean
    elif name_u == 'BYTE':
        qdbc_type = Byte
    elif name_u == 'SHORT':
        qdbc_type = Short
    elif name_u == 'INT' or name_u == 'INTEGER':
        qdbc_type = Int
    elif name_u == 'LONG':
        qdbc_type = Long
    elif name_u == 'FLOAT':
        qdbc_type = Float
    elif name_u == 'DOUBLE':
        qdbc_type = Double
    elif name_u == 'SYMBOL':
        qdbc_type = Symbol
    elif name_u == 'STRING':
        qdbc_type = String
    elif name_u == 'TEXT':
        qdbc_type = String
    elif name_u == 'VARCHAR':
        qdbc_type = String
    elif name_u == 'CHAR':
        qdbc_type = Char
    elif name_u == 'LONG256':
        qdbc_type = Long256
    elif name_u == 'UUID':
        qdbc_type = UUID
    elif name_u == 'DATE':
        qdbc_type = Date
    elif name_u == 'TIMESTAMP':
        qdbc_type = Timestamp
    elif 'GEOHASH' in name_u and '(' in name_u and ')' in name_u:
        open_p = name_u.index('(')
        close_p = name_u.index(')')
        description = name_u[open_p + 1:close_p]
        bits = int(description[:-1])
        if description[-1].upper() == 'C':
            bits *= 5
        qdbc_type = geohash_type(bits)
    return qdbc_type() if qdbc_type else None


# ===== QUESTDB ENGINE =====

class QDBTableEngine(SchemaEventTarget, Traversible):
    def __init__(
            self,
            table_name: str,
            ts_col_name: str = None,
            partition_by: PartitionBy = PartitionBy.DAY,
            is_wal: bool = True
    ):
        Traversible.__init__(self)
        self.name = table_name
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


# ===== QUESTDB DIALECT TYPES =====


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
        if not isinstance(column.type, QDBTypeMixin):
            raise ArgumentError('Column type is not a valid QuestDB type')
        return column.type.column_spec(column.name)


class QDBSQLCompiler(SQLCompiler):
    def _is_safe_for_fast_insert_values_helper(self):
        return True


class QDBInspector(Inspector):
    def reflecttable(
            self,
            table,
            include_columns,
            exclude_columns=(),
            resolve_fks=True,
            _extend_on=None,
    ):
        # backward compatibility SQLAlchemy 1.3
        return self.reflect_table(table, include_columns, exclude_columns, resolve_fks, _extend_on)

    def reflect_table(
            self,
            table,
            include_columns=None,
            exclude_columns=None,
            resolve_fks=False,
            _extend_on=None,
    ):
        table_name = table.name
        result_set = self.bind.execute(f"tables() WHERE name = '{table_name}'")
        if not result_set:
            raise NoResultFound(f"Table '{table_name}' does not exist")
        table_attrs = result_set.first()
        col_ts_name = table_attrs['designatedTimestamp']
        partition_by = PartitionBy[table_attrs['partitionBy']]
        is_wal = table_attrs['walEnabled'] == True
        for row in self.bind.execute(f"table_columns('{table_name}')"):
            col_name = row[0]
            if include_columns and col_name not in include_columns:
                continue
            if exclude_columns and col_name in exclude_columns:
                continue
            col_type = resolve_type_from_name(row[1])
            if col_ts_name and col_ts_name.upper() == col_name.upper():
                table.append_column(sqla.Column(col_name, col_type, primary_key=True))
            else:
                table.append_column(sqla.Column(col_name, col_type))
        table.engine = QDBTableEngine(table_name, col_ts_name, partition_by, is_wal)
        table.metadata = sqla.MetaData()

    def get_columns(self, table_name, schema=None, **kw):
        result_set = self.bind.execute(f"table_columns('{table_name}')")
        if not result_set:
            raise NoResultFound(f"Table '{table_name}' does not exist")
        return [{
            'name': row[0],
            'type': resolve_type_from_name(row[1]),
            'nullable': True,
            'autoincrement': False,
            'persisted': True
        } for row in result_set]


# class QuestDBDialect(PGDialect_psycopg2, abc.ABC):
class QuestDBDialect(PGDialect_psycopg2, abc.ABC):
    name = 'questdb'
    psycopg2_version = (2, 9)
    default_schema_name = 'public'
    statement_compiler = QDBSQLCompiler
    ddl_compiler = QDBDDLCompiler
    type_compiler = GenericTypeCompiler
    inspector = QDBInspector
    preparer = QDBIdentifierPreparer
    supports_schemas = False
    supports_statement_cache = False
    supports_server_side_cursors = False
    supports_views = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_comments = True
    inline_comments = False
    postfetch_lastrowid = False
    non_native_boolean_check_constraint = False
    max_identifier_length = 255
    _user_defined_max_identifier_length = 255
    supports_multivalues_insert = True
    supports_is_distinct_from = False

    @classmethod
    def dbapi(cls):
        import questdb_connect as dbapi
        return dbapi

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
