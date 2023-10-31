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
