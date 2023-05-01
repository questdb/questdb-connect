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
import os
import time
from typing import NamedTuple

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

import pytest
import questdb_connect.dialect as qdbc
from questdb_connect import types
from sqlalchemy import Column, MetaData, text
from sqlalchemy.orm import declarative_base

os.environ['TZ'] = 'UTC'
time.tzset()

TEST_TABLE_NAME = 'all_types_table'


class TestConfig(NamedTuple):
    host: str
    port: int
    username: str
    password: str
    database: str
    __test__ = True


@pytest.fixture(scope='session', autouse=True, name='test_config')
def test_config_fixture() -> TestConfig:
    return TestConfig(
        host=os.environ.get('QUESTDB_CONNECT_HOST', 'localhost'),
        port=int(os.environ.get('QUESTDB_CONNECT_PORT', '8812')),
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


@pytest.fixture(autouse=True, name='test_model')
def test_model_fixture(test_engine):
    Base = declarative_base(metadata=MetaData())

    class TableModel(Base):
        __tablename__ = TEST_TABLE_NAME
        __table_args__ = (qdbc.QDBTableEngine(TEST_TABLE_NAME, 'col_ts', types.PartitionBy.DAY, is_wal=True),)
        col_boolean = Column('col_boolean', types.Boolean)
        col_byte = Column('col_byte', types.Byte)
        col_short = Column('col_short', types.Short)
        col_int = Column('col_int', types.Int)
        col_long = Column('col_long', types.Long)
        col_float = Column('col_float', types.Float)
        col_double = Column('col_double', types.Double)
        col_symbol = Column('col_symbol', types.Symbol)
        col_string = Column('col_string', types.String)
        col_char = Column('col_char', types.Char)
        col_uuid = Column('col_uuid', types.UUID)
        col_date = Column('col_date', types.Date)
        col_ts = Column('col_ts', types.Timestamp, primary_key=True)
        col_geohash = Column('col_geohash', types.GeohashInt)
        col_long256 = Column('col_long256', types.Long256)

    TableModel.metadata.drop_all(test_engine)
    TableModel.metadata.create_all(test_engine)
    return TableModel


def collect_select_all(session, expected_rows) -> str:
    while True:
        rs = session.execute(text(f'select * from public.{TEST_TABLE_NAME} order by 1 asc'))
        if rs.rowcount == expected_rows:
            return '\n'.join(str(row) for row in rs)


def collect_select_all_raw_connection(test_engine, expected_rows) -> str:
    conn = test_engine.raw_connection()
    try:
        while True:
            with conn.cursor() as cursor:
                cursor.execute(f'select * from public.{TEST_TABLE_NAME} order by 1 asc')
                if cursor.rowcount == expected_rows:
                    return '\n'.join(str(row) for row in cursor)
    finally:
        if conn:
            conn.close()
