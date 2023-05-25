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

import sqlalchemy
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.sql.compiler import GenericTypeCompiler

from .compilers import QDBDDLCompiler, QDBSQLCompiler
from .identifier_preparer import QDBIdentifierPreparer
from .inspector import QDBInspector

# ===== SQLAlchemy Dialect ======
# https://docs.sqlalchemy.org/en/14/ apache-superset requires SQLAlchemy 1.4


def connection_uri(
    host: str, port: str, username: str, password: str, database: str = "main"
):
    return f"questdb://{username}:{password}@{host}:{port}/{database}"


def create_engine(
    host: str, port: int, username: str, password: str, database: str = "main"
):
    return sqlalchemy.create_engine(
        connection_uri(host, port, username, password, database),
        future=False,
        hide_parameters=False,
        implicit_returning=False,
        isolation_level="REPEATABLE READ",
    )


class QuestDBDialect(PGDialect_psycopg2, abc.ABC):
    name = "questdb"
    psycopg2_version = (2, 9)
    default_schema_name = "public"
    statement_compiler = QDBSQLCompiler
    ddl_compiler = QDBDDLCompiler
    type_compiler = GenericTypeCompiler
    inspector = QDBInspector
    preparer = QDBIdentifierPreparer
    supports_schemas = False
    supports_statement_cache = False
    supports_server_side_cursors = False
    supports_native_boolean = True
    supports_views = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_comments = True
    inline_comments = False
    postfetch_lastrowid = False
    non_native_boolean_check_constraint = False
    max_identifier_length = 255
    _user_defined_max_identifier_length = 255
    _has_native_hstore = False
    supports_is_distinct_from = False

    @classmethod
    def dbapi(cls):
        import questdb_connect as dbapi

        return dbapi

    def get_schema_names(self, conn, **kw):
        return ["public"]

    def get_table_names(self, conn, schema=None, **kw):
        return [row.table for row in self._exec(conn, "SHOW TABLES")]

    def has_table(self, conn, table_name, schema=None):
        return self._exec(conn, f"tables() WHERE name='{table_name}'").rowcount == 1

    @sqlalchemy.engine.reflection.cache
    def get_columns(self, conn, table_name, schema=None, **kw):
        return self.inspector.format_table_columns(
            table_name, self._exec(conn, f"table_columns('{table_name}')")
        )

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(
        self,
        conn,
        table_name,
        schema=None,
        postgresql_ignore_search_path=False,
        **kw,
    ):
        return []

    def get_temp_table_names(self, conn, **kw):
        return []

    def get_view_names(self, conn, schema=None, **kw):
        return []

    def get_temp_view_names(self, conn, schema=None, **kw):
        return []

    def get_view_definition(self, conn, view_name, schema=None, **kw):
        pass

    def get_indexes(self, conn, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, conn, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, conn, table_name, schema=None, **kw):
        return []

    def has_sequence(self, conn, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, conn, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, conn, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, conn, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, conn, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, conn):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return None

    def _exec(self, conn, sql_query):
        return conn.execute(sqlalchemy.text(sql_query))
