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

import sqlalchemy
from sqlalchemy.exc import ArgumentError

_GEOHASH_MAX_BITS = 61


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
    __visit_name__ = 'QDBTypeMixin'

    @classmethod
    def matches_type_name(cls, type_name):
        return cls if type_name == cls.__visit_name__ else None

    def column_spec(self, column_name):
        return f"'{column_name}' {self.__visit_name__}"


class Boolean(QDBTypeMixin, sqlalchemy.Boolean):
    __visit_name__ = 'BOOLEAN'


class Byte(QDBTypeMixin, sqlalchemy.Integer):
    __visit_name__ = 'BYTE'


class Short(QDBTypeMixin, sqlalchemy.Integer):
    __visit_name__ = 'SHORT'


class Int(QDBTypeMixin, sqlalchemy.Integer):
    __visit_name__ = 'INT'


class Long(QDBTypeMixin, sqlalchemy.Integer):
    __visit_name__ = 'LONG'


class Float(QDBTypeMixin, sqlalchemy.Float):
    __visit_name__ = 'FLOAT'


class Double(QDBTypeMixin, sqlalchemy.Float):
    __visit_name__ = 'DOUBLE'


class Symbol(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = 'SYMBOL'


class String(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = 'STRING'


class Char(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = 'CHAR'


class Long256(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = 'LONG256'


class UUID(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = 'UUID'


class Date(QDBTypeMixin, sqlalchemy.Date):
    __visit_name__ = 'DATE'


class Timestamp(QDBTypeMixin, sqlalchemy.DateTime):
    __visit_name__ = 'TIMESTAMP'


def geohash_type(bits):
    """Factory for Geohash types"""
    type_name = geohash_type_name(bits)
    geohash_class = _TYPE_CACHE.get(type_name)
    if not geohash_class:
        geohash_class = eval(f'Geohash{bits}')
        _TYPE_CACHE[type_name] = geohash_class
    return geohash_class


def geohash_type_name(bits):
    if not isinstance(bits, int) or bits < 0 or bits >= _GEOHASH_MAX_BITS:
        raise ArgumentError(f'geohash precision should be int [0, {_GEOHASH_MAX_BITS - 1}]')
    return f'GEOHASH({bits}b)'


def resolve_type_from_name(type_name):
    type_class = _TYPE_CACHE.get(type_name)
    if not type_class:
        t_name = type_name.upper()
        for candidate_class in basic_type_classes:
            type_class = candidate_class.matches_type_name(t_name)
            if type_class:
                _TYPE_CACHE[t_name] = type_class
                return type_class
        if 'GEOHASH' in t_name and '(' in t_name and ')' in t_name:
            open_p = t_name.index('(')
            close_p = t_name.index(')')
            description = t_name[open_p + 1:close_p]
            bits = int(description[:-1])
            if description[-1] == 'C':
                bits *= 5
            t_name = geohash_type_name(bits)
            type_class = eval(f'Geohash{bits}')
        if not type_class:
            raise ArgumentError(f'unsupported type: {type_name}')
        _TYPE_CACHE[t_name] = type_class
    return type_class


_TYPE_CACHE = {
    # key:   '__visit_name__' of the implementor of QDBTypeMixin
    # value: implementor class itself
}

basic_type_classes = [
    Long256, Boolean, Byte, Short, Char, Int, Long, UUID, Float, Double, Date, Timestamp, Symbol, String
]


class Geohash1(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(1)


class Geohash2(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(2)


class Geohash3(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(3)


class Geohash4(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(4)


class Geohash5(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(5)


class Geohash6(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(6)


class Geohash7(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(7)


class Geohash8(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(8)


class Geohash9(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(9)


class Geohash10(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(10)


class Geohash11(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(11)


class Geohash12(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(12)


class Geohash13(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(13)


class Geohash14(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(14)


class Geohash15(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(15)


class Geohash16(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(16)


class Geohash17(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(17)


class Geohash18(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(18)


class Geohash19(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(19)


class Geohash20(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(20)


class Geohash21(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(21)


class Geohash22(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(22)


class Geohash23(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(23)


class Geohash24(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(24)


class Geohash25(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(25)


class Geohash26(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(26)


class Geohash27(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(27)


class Geohash28(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(28)


class Geohash29(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(29)


class Geohash30(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(30)


class Geohash31(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(31)


class Geohash32(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(32)


class Geohash33(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(33)


class Geohash34(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(34)


class Geohash35(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(35)


class Geohash36(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(36)


class Geohash37(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(37)


class Geohash38(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(38)


class Geohash39(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(39)


class Geohash40(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(40)


class Geohash41(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(41)


class Geohash42(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(42)


class Geohash43(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(43)


class Geohash44(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(44)


class Geohash45(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(45)


class Geohash46(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(46)


class Geohash47(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(47)


class Geohash48(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(48)


class Geohash49(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(49)


class Geohash50(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(50)


class Geohash51(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(51)


class Geohash52(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(52)


class Geohash53(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(53)


class Geohash54(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(54)


class Geohash55(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(55)


class Geohash56(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(56)


class Geohash57(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(57)


class Geohash58(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(58)


class Geohash59(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(59)


class Geohash60(QDBTypeMixin, sqlalchemy.String):
    __visit_name__ = geohash_type_name(60)
