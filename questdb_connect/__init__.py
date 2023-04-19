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
import enum

import psycopg2
import sqlalchemy as sqla

# ===== DBAPI =====

apilevel = '2.0'
threadsafety = 2
paramstyle = 'pyformat'


def connect(**kwargs):
    host = kwargs.get('host') or '127.0.0.1'
    port = kwargs.get('port') or 8812
    user = kwargs.get('username') or 'admin'
    passwd = kwargs.get('password') or 'quest'
    return psycopg2.connect(host=host, port=port, user=user, password=passwd)


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
        raise AttributeError(f'bits shoultdbe an int [0, {_GEOHASH_MAX_BITS}]')

    class GeohashWithPrecision(sqla.String, QDBType):
        __visit_name__ = f'GEOHASH({bits}b)'
        bit_precision = bits

    return GeohashWithPrecision
