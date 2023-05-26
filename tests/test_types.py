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

import questdb_connect as qdbc


def test_resolve_type_from_name():
    for type_class in qdbc.QUESTDB_TYPES:
        resolved_class = qdbc.resolve_type_from_name(type_class.__visit_name__)
        assert type_class.__visit_name__ == resolved_class.__visit_name__
        assert isinstance(type_class(), resolved_class)
        assert isinstance(resolved_class(), type_class)

    for n in range(1, 61):
        g_name = qdbc.geohash_type_name(n)
        g_class = qdbc.resolve_type_from_name(g_name)
        assert isinstance(g_class(), qdbc.geohash_class(n))


def test_superset_default_mappings():
    default_column_type_mappings = (
        (re.compile("^LONG256", re.IGNORECASE), qdbc.Long256),
        (re.compile("^BOOLEAN", re.IGNORECASE), qdbc.Boolean),
        (re.compile("^BYTE", re.IGNORECASE), qdbc.Byte),
        (re.compile("^SHORT", re.IGNORECASE), qdbc.Short),
        (re.compile("^INT", re.IGNORECASE), qdbc.Int),
        (re.compile("^LONG", re.IGNORECASE), qdbc.Long),
        (re.compile("^FLOAT", re.IGNORECASE), qdbc.Float),
        (re.compile("^DOUBLE'", re.IGNORECASE), qdbc.Double),
        (re.compile("^SYMBOL", re.IGNORECASE), qdbc.Symbol),
        (re.compile("^STRING", re.IGNORECASE), qdbc.String),
        (re.compile("^UUID", re.IGNORECASE), qdbc.UUID),
        (re.compile("^CHAR", re.IGNORECASE), qdbc.Char),
        (re.compile("^TIMESTAMP", re.IGNORECASE), qdbc.Timestamp),
        (re.compile("^DATE", re.IGNORECASE), qdbc.Date)
    )
    for type_class in qdbc.QUESTDB_TYPES:
        for pattern, _expected_type in default_column_type_mappings:
            matching_name = pattern.match(type_class.__visit_name__)
            if matching_name:
                resolved_class = qdbc.resolve_type_from_name(matching_name.group(0))
                assert type_class.__visit_name__ == resolved_class.__visit_name__
                assert isinstance(type_class(), resolved_class)
                assert isinstance(resolved_class(), type_class)
                break
    geohash_pattern = re.compile(r"^GEOHASH\(\d+[b|c]\)", re.IGNORECASE)
    for n in range(1, 61):
        g_name = qdbc.geohash_type_name(n)
        matching_name = geohash_pattern.match(g_name).group(0)
        assert matching_name == g_name
        g_class = qdbc.resolve_type_from_name(g_name)
        assert isinstance(g_class(), qdbc.geohash_class(n))
