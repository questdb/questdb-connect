import os
import time
from typing import NamedTuple

import pytest
import questdb_connect as qdbc
from sqlalchemy import Column, MetaData, text
from sqlalchemy.orm import declarative_base

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

ALL_TYPES_TABLE_NAME = 'all_types_table'
METRICS_TABLE_NAME = 'metrics_table'


class TestConfig(NamedTuple):
    host: str
    port: str
    username: str
    password: str
    database: str
    __test__ = True


@pytest.fixture(scope='session', autouse=True, name='test_config')
def test_config_fixture() -> TestConfig:
    return TestConfig(
        host=os.environ.get('QUESTDB_CONNECT_HOST', 'localhost'),
        port=os.environ.get('QUESTDB_CONNECT_PORT', '8812'),
        username=os.environ.get('QUESTDB_CONNECT_USER', 'admin'),
        password=os.environ.get('QUESTDB_CONNECT_PASSWORD', 'quest'),
        database=os.environ.get('QUESTDB_CONNECT_DATABASE', 'main')
    )


@pytest.fixture(scope='module', name='test_engine')
def test_engine_fixture(test_config: TestConfig):
    engine = None
    try:
        engine = qdbc.create_engine(
            test_config.host,
            test_config.port,
            test_config.username,
            test_config.password,
            test_config.database)
        return engine
    finally:
        if engine:
            engine.dispose()
            del engine

@pytest.fixture(scope='module', name='superset_test_engine')
def test_superset_engine_fixture(test_config: TestConfig):
    engine = None
    try:
        engine = qdbc.create_superset_engine(
            test_config.host,
            test_config.port,
            test_config.username,
            test_config.password,
            test_config.database)
        return engine
    finally:
        if engine:
            engine.dispose()
            del engine

@pytest.fixture(autouse=True, name='test_model')
def test_model_fixture(test_engine):
    Base = declarative_base(metadata=MetaData())

    class TableModel(Base):
        __tablename__ = ALL_TYPES_TABLE_NAME
        __table_args__ = (qdbc.QDBTableEngine(ALL_TYPES_TABLE_NAME, 'col_ts', qdbc.PartitionBy.DAY, is_wal=True),)
        col_boolean = Column('col_boolean', qdbc.Boolean)
        col_byte = Column('col_byte', qdbc.Byte)
        col_short = Column('col_short', qdbc.Short)
        col_int = Column('col_int', qdbc.Int)
        col_long = Column('col_long', qdbc.Long)
        col_float = Column('col_float', qdbc.Float)
        col_double = Column('col_double', qdbc.Double)
        col_symbol = Column('col_symbol', qdbc.Symbol)
        col_string = Column('col_string', qdbc.String)
        col_char = Column('col_char', qdbc.Char)
        col_uuid = Column('col_uuid', qdbc.UUID)
        col_date = Column('col_date', qdbc.Date)
        col_ts = Column('col_ts', qdbc.Timestamp, primary_key=True)
        col_geohash = Column('col_geohash', qdbc.GeohashInt)
        col_long256 = Column('col_long256', qdbc.Long256)
        col_varchar = Column('col_varchar', qdbc.Varchar)

    Base.metadata.drop_all(test_engine)
    Base.metadata.create_all(test_engine)
    return TableModel


@pytest.fixture(autouse=True, name='test_metrics')
def test_metrics_fixture(test_engine):
    Base = declarative_base(metadata=MetaData())

    class TableMetrics(Base):
        __tablename__ = METRICS_TABLE_NAME
        __table_args__ = (
            qdbc.QDBTableEngine(
                METRICS_TABLE_NAME,
                'ts',
                qdbc.PartitionBy.HOUR,
                is_wal=True,
                dedup_upsert_keys=('source', 'attr_name', 'ts')
            ),
        )
        source = Column(qdbc.Symbol)
        attr_name = Column(qdbc.Symbol)
        attr_value = Column(qdbc.Double)
        ts = Column(qdbc.Timestamp, primary_key=True)

    Base.metadata.drop_all(test_engine)
    Base.metadata.create_all(test_engine)
    return TableMetrics


def collect_select_all(session, expected_rows) -> str:
    session.commit()
    while True:
        rs = session.execute(text(f'select * from public.{ALL_TYPES_TABLE_NAME} order by 1 asc'))
        if rs.rowcount == expected_rows:
            return '\n'.join(str(row) for row in rs)


def wait_until_table_is_ready(test_engine, table_name, expected_rows, timeout=10):
    """
    Wait until a table has the expected number of rows, with timeout.

    Args:
        test_engine: SQLAlchemy engine
        table_name: Name of the table to check
        expected_rows: Expected number of rows
        timeout: Maximum time to wait in seconds (default: 10 seconds)

    Returns:
        bool: True if table is ready, False if timeout occurred

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If there's a database error
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        with test_engine.connect() as conn:
            result = conn.execute(text(f'SELECT count(*) FROM {table_name}'))
            row = result.fetchone()
            if row and row[0] == expected_rows:
                return True

            print(f'Waiting for table {table_name} to have {expected_rows} rows, current: {row[0] if row else 0}')
            time.sleep(0.01)  # Wait 10ms between checks
    return False


def collect_select_all_raw_connection(test_engine, expected_rows) -> str:
    conn = test_engine.raw_connection()
    try:
        while True:
            with conn.cursor() as cursor:
                cursor.execute(f'select * from public.{ALL_TYPES_TABLE_NAME} order by 1 asc')
                if cursor.rowcount == expected_rows:
                    return '\n'.join(str(row) for row in cursor)
    finally:
        if conn:
            conn.close()
