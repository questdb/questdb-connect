import enum
import re


class PartitionBy(enum.Enum):
    DAY = 0
    MONTH = 1
    YEAR = 2
    NONE = 3
    HOUR = 4
    WEEK = 5


def remove_public_schema(query):
    if query and isinstance(query, str) and "public" in query:
        return re.sub(_PUBLIC_SCHEMA_FILTER, "", query)
    return query


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


_PUBLIC_SCHEMA_FILTER = re.compile(
    r"(')?(public(?(1)\1|)\.)", re.IGNORECASE | re.MULTILINE
)
_QUOTES = ("'", '"')
