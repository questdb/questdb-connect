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

from .common import quote_identifier


def _none(_ignore):
    return None


_special_chars = {
    "(",
    ")",
    "[",
    "[]",
    "{",
    "}",
    "'",
    '"',
    ":",
    ";",
    ".",
    "!",
    "%",
    "&",
    "*",
    "$",
    "@",
    "~",
    "^",
    "-",
    "?",
    "/",
    "\\",
    " ",
    "\t",
    "\r",
    "\n",
}


def _has_special_char(_value):
    for candidate in _value:
        if candidate in _special_chars:
            return True
    return False


class QDBIdentifierPreparer(sqlalchemy.sql.compiler.IdentifierPreparer, abc.ABC):
    schema_for_object = staticmethod(_none)

    def __init__(
        self,
        dialect,
        initial_quote='"',
        final_quote=None,
        escape_quote='"',
        quote_case_sensitive_collations=False,
        omit_schema=True,
    ):
        super().__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )

    def quote_identifier(self, value):
        return quote_identifier(value)

    def _requires_quotes(self, _value):
        return _value and _has_special_char(_value)

    def format_schema(self, name):
        """Prepare a quoted schema name."""
        return ""

    def format_table(self, table, use_schema=True, name=None):
        """Prepare a quoted table and schema name."""
        return quote_identifier(name if name else table.name)
