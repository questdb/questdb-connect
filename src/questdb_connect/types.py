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
from __future__ import annotations

import enum

import sqlalchemy as sqla
from sqlalchemy.exc import ArgumentError

# ===== QUESTDB PARTITION TYPE =====


class PartitionBy(enum.Enum):
    DAY = 0
    MONTH = 1
    YEAR = 2
    NONE = 3
    HOUR = 4
    WEEK = 5


# ===== QUESTDB DATA TYPES =====

_QUOTES = ("'", '"')
_GEOHASH_BYTE_MAX = 8
_GEOHASH_SHORT_MAX = 16
_GEOHASH_INT_MAX = 32
_GEOHASH_LONG_BITS = 60
_TYPE_CACHE = {
    # key:   '__visit_name__' of the implementor of QDBTypeMixin
    # value: implementor class itself
}


def geohash_type_name(bits):
    if not isinstance(bits, int) or bits < 0 or bits > _GEOHASH_LONG_BITS:
        raise ArgumentError(f'geohash precision must be int [0, {_GEOHASH_LONG_BITS}]')
    if 0 < bits <= _GEOHASH_BYTE_MAX:
        return f'GEOHASH(8b)'
    elif _GEOHASH_BYTE_MAX < bits <= _GEOHASH_SHORT_MAX:
        return 'GEOHASH(3c)'
    elif _GEOHASH_SHORT_MAX < bits <= _GEOHASH_INT_MAX:
        return 'GEOHASH(6c)'
    return f'GEOHASH(12c)'


def geohash_class(bits):
    if not isinstance(bits, int) or bits < 0 or bits > _GEOHASH_LONG_BITS:
        raise ArgumentError(f'geohash precision must be int [0, {_GEOHASH_LONG_BITS}]')
    if 0 < bits <= _GEOHASH_BYTE_MAX:
        return GeohashByte
    elif _GEOHASH_BYTE_MAX < bits <= _GEOHASH_SHORT_MAX:
        return GeohashShort
    elif _GEOHASH_SHORT_MAX < bits <= _GEOHASH_INT_MAX:
        return GeohashInt
    return GeohashLong


def quote_identifier(identifier: str):
    if not identifier:
        return None
    first = 0
    last = len(identifier)
    if identifier[first] in _QUOTES:
        first += 1
    if identifier[last - 1] in _QUOTES:
        last -= 1
    return f'"{identifier[first:last]}"'


class QDBTypeMixin(sqla.types.TypeDecorator):
    __visit_name__ = 'QDBTypeMixin'
    impl = sqla.types.String
    cache_ok = True

    @classmethod
    def matches_type_name(cls, type_name):
        return cls if type_name == cls.__visit_name__ else None

    def column_spec(self, column_name):
        return f"{quote_identifier(column_name)} {self.__visit_name__}"


class Boolean(QDBTypeMixin):
    __visit_name__ = 'BOOLEAN'
    impl = sqla.types.Boolean
    type_code = 1


class Byte(QDBTypeMixin):
    __visit_name__ = 'BYTE'
    impl = sqla.types.Integer
    type_code = 2


class Short(QDBTypeMixin):
    __visit_name__ = 'SHORT'
    type_code = 3
    impl = sqla.types.Integer


class Char(QDBTypeMixin):
    __visit_name__ = 'CHAR'
    type_code = 4


class Int(QDBTypeMixin):
    __visit_name__ = 'INT'
    type_code = 5
    impl = sqla.types.Integer


class Long(QDBTypeMixin):
    __visit_name__ = 'LONG'
    type_code = 6
    impl = sqla.types.Integer


class Date(QDBTypeMixin):
    __visit_name__ = 'DATE'
    type_code = 7
    impl = sqla.types.Date


class Timestamp(QDBTypeMixin):
    __visit_name__ = 'TIMESTAMP'
    type_code = 8
    impl = sqla.types.DateTime


class Float(QDBTypeMixin):
    __visit_name__ = 'FLOAT'
    type_code = 9
    impl = sqla.types.Float


class Double(QDBTypeMixin):
    __visit_name__ = 'DOUBLE'
    type_code = 10
    impl = sqla.types.Float


class String(QDBTypeMixin):
    __visit_name__ = 'STRING'
    type_code = 11


class Symbol(QDBTypeMixin):
    __visit_name__ = 'SYMBOL'
    type_code = 12


class Long256(QDBTypeMixin):
    __visit_name__ = 'LONG256'
    type_code = 13


class GeohashByte(QDBTypeMixin):
    __visit_name__ = geohash_type_name(8)
    type_code = 14


class GeohashShort(QDBTypeMixin):
    __visit_name__ = geohash_type_name(16)
    type_code = 15


class GeohashInt(QDBTypeMixin):
    __visit_name__ = geohash_type_name(32)
    type_code = 16


class GeohashLong(QDBTypeMixin):
    __visit_name__ = geohash_type_name(60)
    type_code = 17


class UUID(QDBTypeMixin):
    __visit_name__ = 'UUID'
    type_code = 19


QUESTDB_TYPES = [
    Long256,
    Boolean,
    Byte,
    Short,
    Char,
    Int,
    Long,
    UUID,
    Float,
    Double,
    Date,
    Timestamp,
    Symbol,
    String,
    GeohashByte,
    GeohashInt,
    GeohashShort,
    GeohashLong,
]


def resolve_type_from_name(type_name):
    type_class = _TYPE_CACHE.get(type_name)
    if not type_class:
        for candidate_class in QUESTDB_TYPES:
            type_class = candidate_class.matches_type_name(type_name)
            if type_class:
                _TYPE_CACHE[type_name] = type_class
                break
            elif 'GEOHASH' in type_name.upper() and '(' in type_name and ')' in type_name:
                open_p = type_name.index('(')
                close_p = type_name.index(')')
                description = type_name[open_p + 1:close_p]
                g_size = int(description[:-1])
                if description[-1] in ('C', 'c'):
                    g_size *= 5
                type_class = geohash_class(g_size)
                break
    if not type_class:
        raise ArgumentError(f'unsupported type: {type_name}')
    return type_class
