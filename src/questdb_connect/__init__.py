import os
import time

import psycopg2

from questdb_connect.common import PartitionBy, remove_public_schema
from questdb_connect.compilers import QDBDDLCompiler, QDBSQLCompiler
from questdb_connect.dialect import QuestDBDialect, connection_uri, create_engine
from questdb_connect.identifier_preparer import QDBIdentifierPreparer
from questdb_connect.inspector import QDBInspector
from questdb_connect.keywords_functions import get_functions_list, get_keywords_list
from questdb_connect.table_engine import QDBTableEngine
from questdb_connect.types import (
    QUESTDB_TYPES,
    UUID,
    Boolean,
    Byte,
    Char,
    Date,
    Double,
    Float,
    GeohashByte,
    GeohashInt,
    GeohashLong,
    GeohashShort,
    Int,
    IPv4,
    Long,
    Long128,
    Long256,
    QDBTypeMixin,
    Short,
    String,
    Symbol,
    Timestamp,
    geohash_class,
    geohash_type_name,
    resolve_type_from_name,
)

# QuestDB timestamps: https://questdb.io/docs/guides/working-with-timestamps-timezones/
# The native timestamp format used by QuestDB is a Unix timestamp in microsecond resolution.
# Although timestamps in nanoseconds will be parsed, the output will be truncated to
# microseconds. QuestDB does not store time zone information alongside timestamp values
# and therefore it should be assumed that all timestamps are in UTC.
if hasattr(time, "tzset"):
    os.environ["TZ"] = "UTC"
    time.tzset()

# ===== DBAPI =====
# https://peps.python.org/pep-0249/

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


class Error(Exception):
    pass


class Cursor(psycopg2.extensions.cursor):
    def execute(self, query, vars=None):
        """execute(query, vars=None) -- Execute query with bound vars."""
        return super().execute(remove_public_schema(query), vars)


def cursor_factory(*args, **kwargs):
    return Cursor(*args, **kwargs)


def connect(**kwargs):
    host = kwargs.get("host") or "127.0.0.1"
    port = kwargs.get("port") or 8812
    user = kwargs.get("user") or "admin"
    password = kwargs.get("password") or "quest"
    database = kwargs.get("database") or "main"
    conn = psycopg2.connect(
        cursor_factory=cursor_factory,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    # retrieve and cache function names and keywords lists
    get_keywords_list(conn)
    get_functions_list(conn)
    return conn
