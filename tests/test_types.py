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
import re

from questdb_connect import types


def test_resolve_type_from_name():
    for type_class in types.QUESTDB_TYPES:
        resolved_class = types.resolve_type_from_name(type_class.__visit_name__)
        assert type_class.__visit_name__ == resolved_class.__visit_name__
        assert isinstance(type_class(), resolved_class)
        assert isinstance(resolved_class(), type_class)

    for n in range(1, 61):
        g_name = types.geohash_type_name(n)
        g_class = types.resolve_type_from_name(g_name)
        assert isinstance(g_class(), types.geohash_class(n))


def test_superset_default_mappings():
    default_column_type_mappings = (
        (re.compile("^LONG256", re.IGNORECASE), types.Long256),
        (re.compile("^BOOLEAN", re.IGNORECASE), types.Boolean),
        (re.compile("^BYTE", re.IGNORECASE), types.Byte),
        (re.compile("^SHORT", re.IGNORECASE), types.Short),
        (re.compile("^INT", re.IGNORECASE), types.Int),
        (re.compile("^LONG", re.IGNORECASE), types.Long),
        (re.compile("^FLOAT", re.IGNORECASE), types.Float),
        (re.compile("^DOUBLE'", re.IGNORECASE), types.Double),
        (re.compile("^SYMBOL", re.IGNORECASE), types.Symbol),
        (re.compile("^STRING", re.IGNORECASE), types.String),
        (re.compile("^UUID", re.IGNORECASE), types.UUID),
        (re.compile("^CHAR", re.IGNORECASE), types.Char),
        (re.compile("^TIMESTAMP", re.IGNORECASE), types.Timestamp),
        (re.compile("^DATE", re.IGNORECASE), types.Date)
    )
    for type_class in types.QUESTDB_TYPES:
        for pattern, _expected_type in default_column_type_mappings:
            matching_name = pattern.match(type_class.__visit_name__)
            if matching_name:
                resolved_class = types.resolve_type_from_name(matching_name.group(0))
                assert type_class.__visit_name__ == resolved_class.__visit_name__
                assert isinstance(type_class(), resolved_class)
                assert isinstance(resolved_class(), type_class)
                break
    geohash_pattern = re.compile(r"^GEOHASH\(\d+[b|c]\)", re.IGNORECASE)
    for n in range(1, 61):
        g_name = types.geohash_type_name(n)
        matching_name = geohash_pattern.match(g_name).group(0)
        assert matching_name == g_name
        g_class = types.resolve_type_from_name(g_name)
        assert isinstance(g_class(), types.geohash_class(n))
