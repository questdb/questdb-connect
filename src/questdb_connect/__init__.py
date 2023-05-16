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

import psycopg2

# ===== DBAPI =====
# https://peps.python.org/pep-0249/

apilevel = '2.0'
threadsafety = 2
paramstyle = 'pyformat'
public_schema_filter = re.compile(r"(')?(public(?(1)\1|)\.)", re.IGNORECASE | re.MULTILINE)


def remove_public_schema(query):
    if query and isinstance(query, str) and 'public' in query:
        return re.sub(public_schema_filter, '', query)
    return query


def ts_in_group_by_removing_parse_sql(query: str, timestamp_alias: str = '__timestamp') -> list[str]:
    import sqlparse
    parsed_sql_text = []
    for parsed_sql in sqlparse.parse(query):
        if parsed_sql.get_type().lower() == 'select':
            # remove timestamp column from any group by
            ts_col_name = None
            has_group_by = False
            for tok in parsed_sql.tokens:
                if not ts_col_name and isinstance(tok, sqlparse.sql.IdentifierList):
                    for select_tok in tok.tokens:
                        if timestamp_alias in select_tok.value:
                            ts_col_name = select_tok.normalized
                            break
                elif ts_col_name and tok.value.lower() == 'group by':
                    has_group_by = True
                elif ts_col_name and has_group_by and isinstance(tok, sqlparse.sql.IdentifierList):
                    remove_idx = None
                    comma_idxs = []
                    for idx, group_tok in enumerate(tok.tokens):
                        if group_tok.match(sqlparse.tokens.Punctuation, ','):
                            comma_idxs.append(idx)
                        elif group_tok.normalized == ts_col_name:
                            remove_idx = idx
                    if remove_idx is not None:
                        tok.tokens.pop(remove_idx)
                        if remove_idx == 0:
                            tok.tokens.pop(comma_idxs[0] - 1)
                        else:
                            remove_comma_idx = -1
                            for i, c_i in enumerate(comma_idxs):
                                if c_i > remove_idx:
                                    remove_comma_idx = i - 1
                                    break
                            tok.tokens.pop(comma_idxs[remove_comma_idx])
        parsed_sql_text.append(str(parsed_sql).strip(' ;'))
    return parsed_sql_text


class Error(Exception):
    pass


class Cursor(psycopg2.extensions.cursor):
    def execute(self, query, vars=None):
        return super().execute(remove_public_schema(query), vars)


def cursor_factory(*args, **kwargs):
    return Cursor(*args, **kwargs)


def connect(**kwargs):
    host = kwargs.get('host') or '127.0.0.1'
    port = kwargs.get('port') or 8812
    user = kwargs.get('username') or 'admin'
    password = kwargs.get('password') or 'quest'
    database = kwargs.get('database') or 'main'
    return psycopg2.connect(
        cursor_factory=cursor_factory,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database)
