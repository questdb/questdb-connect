import re

import questdb_connect as qdbc
from questdb_connect.common import quote_identifier


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


def test_symbol_type():
    # Test basic Symbol without parameters
    symbol = qdbc.Symbol()
    assert symbol.__visit_name__ == "SYMBOL"
    assert symbol.compile() == "SYMBOL"
    assert symbol.column_spec("test_col") == "\"test_col\" SYMBOL"

    # Test Symbol with capacity
    symbol_cap = qdbc.Symbol(capacity=128)
    assert symbol_cap.compile() == "SYMBOL CAPACITY 128"
    assert symbol_cap.column_spec("test_col") == "\"test_col\" SYMBOL CAPACITY 128"

    # Test Symbol with cache true
    symbol_cache = qdbc.Symbol(cache=True)
    assert symbol_cache.compile() == "SYMBOL CACHE"
    assert symbol_cache.column_spec("test_col") == "\"test_col\" SYMBOL CACHE"

    # Test Symbol with cache false
    symbol_nocache = qdbc.Symbol(cache=False)
    assert symbol_nocache.compile() == "SYMBOL NOCACHE"
    assert symbol_nocache.column_spec("test_col") == "\"test_col\" SYMBOL NOCACHE"

    # Test Symbol with both parameters
    symbol_full = qdbc.Symbol(capacity=256, cache=True)
    assert symbol_full.compile() == "SYMBOL CAPACITY 256 CACHE"
    assert symbol_full.column_spec("test_col") == "\"test_col\" SYMBOL CAPACITY 256 CACHE"

    # Test inheritance and type resolution
    assert isinstance(symbol, qdbc.QDBTypeMixin)
    resolved_class = qdbc.resolve_type_from_name("SYMBOL")
    assert resolved_class.__visit_name__ == "SYMBOL"
    assert isinstance(symbol, resolved_class)

    # Test that parameters don't affect type resolution
    symbol_with_params = qdbc.Symbol(capacity=128, cache=True)
    assert isinstance(symbol_with_params, resolved_class)
    assert isinstance(resolved_class(), type(symbol_with_params))


def test_symbol_backward_compatibility():
    """Verify that the parametrized Symbol type maintains backward compatibility with older code."""
    # Test all the ways Symbol type could be previously instantiated
    symbol1 = qdbc.Symbol
    symbol2 = qdbc.Symbol()

    # Check that both work in column definitions
    from sqlalchemy import Column, MetaData, Table

    metadata = MetaData()
    test_table = Table(
        'test_table',
        metadata,
        Column('old_style1', symbol1),  # Old style: direct class reference
        Column('old_style2', symbol2),  # Old style: basic instantiation
    )

    # Verify type resolution still works
    for column in test_table.columns:
        # Check inheritance
        assert isinstance(column.type, qdbc.QDBTypeMixin)

        # Check type resolution
        resolved_class = qdbc.resolve_type_from_name("SYMBOL")
        assert isinstance(column.type, resolved_class)

        # Check SQL generation matches old behavior
        assert column.type.compile() == "SYMBOL"
        assert column.type.column_spec(column.name) == f"{quote_identifier(column.name)} SYMBOL"

def test_symbol_type_in_column():
    # Test Symbol type in Column definition
    from sqlalchemy import Column, MetaData, Table

    metadata = MetaData()

    # Create a test table with different Symbol column variations
    test_table = Table(
        'test_table',
        metadata,
        Column('basic_symbol', qdbc.Symbol()),
        Column('symbol_with_capacity', qdbc.Symbol(capacity=128)),
        Column('symbol_with_cache', qdbc.Symbol(cache=True)),
        Column('symbol_with_nocache', qdbc.Symbol(cache=False)),
        Column('symbol_full', qdbc.Symbol(capacity=256, cache=True))
    )

    # Get the create table SQL (implementation-dependent)
    # This part might need adjustment based on your actual SQL compilation logic
    for column in test_table.columns:
        assert isinstance(column.type, qdbc.Symbol)
        assert isinstance(column.type, qdbc.QDBTypeMixin)

        if column.name == 'basic_symbol':
            assert column.type.compile() == "SYMBOL"
        elif column.name == 'symbol_with_capacity':
            assert column.type.compile() == "SYMBOL CAPACITY 128"
        elif column.name == 'symbol_with_cache':
            assert column.type.compile() == "SYMBOL CACHE"
        elif column.name == 'symbol_with_nocache':
            assert column.type.compile() == "SYMBOL NOCACHE"
        elif column.name == 'symbol_full':
            assert column.type.compile() == "SYMBOL CAPACITY 256 CACHE"


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
        (re.compile("^VARCHAR$", re.IGNORECASE), qdbc.Varchar),
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
