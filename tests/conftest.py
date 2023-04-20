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

import pytest
from sqlalchemy import Column, MetaData, text
from sqlalchemy.orm import declarative_base

import questdb_connect as qdbc
from questdb_connect.dialect import QDBEngine, create_engine

os.environ['TZ'] = 'UTC'
time.tzset()


class TestConfig(NamedTuple):
    host: str
    port: int
    username: str
    password: str
    __test__ = False


@pytest.fixture(scope='session', autouse=True, name='test_config')
def test_config_fixture() -> TestConfig:
    host = os.environ.get('QUESTDB_CONNECT_HOST', 'localhost')
    port = int(os.environ.get('QUESTDB_CONNECT_PORT', '8812'))
    username = os.environ.get('QUESTDB_CONNECT_USER', 'admin')
    password = os.environ.get('QUESTDB_CONNECT_PASSWORD', 'quest')
    return TestConfig(host, port, username, password)


@pytest.fixture(scope='module', name='test_engine')
def test_engine_fixture(test_config: TestConfig):
    engine = None
    try:
        engine = create_engine(test_config.host, test_config.port, test_config.username, test_config.password)
        return engine
    finally:
        if engine:
            engine.dispose()
            del engine


@pytest.fixture(autouse=True, name='test_model')
def test_model_fixture(test_engine):
    Base = declarative_base(metadata=MetaData())

    class TableModel(Base):
        __tablename__ = 'all_types_table'
        __table_args__ = (QDBEngine(ts_col_name='col_ts', partition_by=qdbc.PartitionBy.DAY, is_wal=True),)
        col_boolean = Column(qdbc.Boolean)
        col_byte = Column(qdbc.Byte)
        col_short = Column(qdbc.Short)
        col_int = Column(qdbc.Int)
        col_long = Column(qdbc.Long)
        col_float = Column(qdbc.Float)
        col_double = Column(qdbc.Double)
        col_symbol = Column(qdbc.Symbol)
        col_string = Column(qdbc.String)
        col_char = Column(qdbc.Char)
        col_uuid = Column(qdbc.UUID)
        col_date = Column(qdbc.Date)
        col_ts = Column(qdbc.Timestamp, primary_key=True)
        col_geohash = Column(qdbc.geohash_type(40))
        col_long256 = Column(qdbc.Long256)

    TableModel.metadata.drop_all(test_engine)
    TableModel.metadata.create_all(test_engine)
    return TableModel


def collect_select_all(conn, expected_rows) -> str:
    while True:
        rs = conn.execute(text('all_types_table'))
        if rs.rowcount == expected_rows:
            return '\n'.join(str(row) for row in rs)


def collect_select_all_raw_connection(test_engine, expected_rows) -> str:
    conn = test_engine.raw_connection()
    try:
        while True:
            with conn.cursor() as cursor:
                cursor.execute('all_types_table')
                if cursor.rowcount == expected_rows:
                    return '\n'.join(str(row) for row in cursor)
    finally:
        if conn:
            conn.close()
