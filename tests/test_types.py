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
        (re.compile("^BOOLEAN$", re.IGNORECASE), qdbc.Boolean),
        (re.compile("^BYTE$", re.IGNORECASE), qdbc.Byte),
        (re.compile("^SHORT$", re.IGNORECASE), qdbc.Short),
        (re.compile("^CHAR$", re.IGNORECASE), qdbc.Char),
        (re.compile("^INT$", re.IGNORECASE), qdbc.Int),
        (re.compile("^LONG$", re.IGNORECASE), qdbc.Long),
        (re.compile("^DATE$", re.IGNORECASE), qdbc.Date),
        (re.compile("^TIMESTAMP$", re.IGNORECASE), qdbc.Timestamp),
        (re.compile("^FLOAT$", re.IGNORECASE), qdbc.Float),
        (re.compile("^DOUBLE$", re.IGNORECASE), qdbc.Double),
        (re.compile("^STRING$", re.IGNORECASE), qdbc.String),
        (re.compile("^SYMBOL$", re.IGNORECASE), qdbc.Symbol),
        (re.compile("^LONG256$", re.IGNORECASE), qdbc.Long256),
        (re.compile("^UUID$", re.IGNORECASE), qdbc.UUID),
        (re.compile("^LONG118$", re.IGNORECASE), qdbc.UUID),
        (re.compile("^IPV4$", re.IGNORECASE), qdbc.IPv4),
    )
    for type_class in qdbc.QUESTDB_TYPES:
        for pattern, _expected_type in default_column_type_mappings:
            matching_name = pattern.match(type_class.__visit_name__)
            if matching_name:
                print(f"match: {matching_name}, type_class: {type_class}")
                resolved_class = qdbc.resolve_type_from_name(matching_name.group(0))
                assert type_class.__visit_name__ == resolved_class.__visit_name__
                assert isinstance(type_class(), resolved_class)
                assert isinstance(resolved_class(), type_class)
                break
    geohash_pattern = re.compile(r"^GEOHASH\(\d+[b|c]\)$", re.IGNORECASE)
    for n in range(1, 61):
        g_name = qdbc.geohash_type_name(n)
        matching_name = geohash_pattern.match(g_name).group(0)
        assert matching_name == g_name
        g_class = qdbc.resolve_type_from_name(g_name)
        assert isinstance(g_class(), qdbc.geohash_class(n))
