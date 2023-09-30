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
import datetime

import questdb_connect as qdbc
import sqlalchemy as sqla
from sqlalchemy.orm import Session

from tests.conftest import (
    ALL_TYPES_TABLE_NAME,
    METRICS_TABLE_NAME,
    collect_select_all,
    collect_select_all_raw_connection,
)


def test_insert(test_engine, test_model):
    with test_engine.connect() as conn:
        assert test_engine.dialect.has_table(conn, ALL_TYPES_TABLE_NAME)
        assert not test_engine.dialect.has_table(conn, 'scorchio')
        now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
        now_date = now.date()
        expected = ("(True, 8, 12, 13, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                    "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                    "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                    "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')\n"
                    "(True, 8, 12, 13, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                    "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                    "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                    "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')")
        insert_stmt = sqla.insert(test_model).values(
            col_boolean=True,
            col_byte=8,
            col_short=12,
            col_int=13,
            col_long=14,
            col_float=15.234,
            col_double=16.88993244,
            col_symbol='coconut',
            col_string='banana',
            col_char='C',
            col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
            col_date=now_date,
            col_ts=now,
            col_geohash='dfvgsj2vptwu',
            col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a'
        )
        conn.execute(insert_stmt)
        conn.execute(sqla.insert(test_model), {
            'col_boolean': True,
            'col_byte': 8,
            'col_short': 12,
            'col_int': 13,
            'col_long': 14,
            'col_float': 15.234,
            'col_double': 16.88993244,
            'col_symbol': 'coconut',
            'col_string': 'banana',
            'col_char': 'C',
            'col_uuid': '6d5eb038-63d1-4971-8484-30c16e13de5b',
            'col_date': now_date,
            'col_ts': now,
            'col_geohash': 'dfvgsj2vptwu',
            'col_long256': '0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a'
        })
        assert collect_select_all(conn, expected_rows=2) == expected
    assert collect_select_all_raw_connection(test_engine, expected_rows=2) == expected


def test_inspect_1(test_engine, test_model):
    now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
    now_date = now.date()
    session = Session(test_engine)
    try:
        session.add(test_model(
            col_boolean=True,
            col_byte=8,
            col_short=12,
            col_int=0,
            col_long=14,
            col_float=15.234,
            col_double=16.88993244,
            col_symbol='coconut',
            col_string='banana',
            col_char='C',
            col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
            col_date=now_date,
            col_ts=now,
            col_geohash='dfvgsj2vptwu',
            col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a'
        ))
        session.commit()
    finally:
        if session:
            session.close()
    metadata = sqla.MetaData()
    table = sqla.Table(ALL_TYPES_TABLE_NAME, metadata, autoload_with=test_engine)
    table_columns = str([(col.name, col.type, col.primary_key) for col in table.columns])
    assert table_columns == str([
        ('col_boolean', qdbc.Boolean(), False),
        ('col_byte', qdbc.Byte(), False),
        ('col_short', qdbc.Short(), False),
        ('col_int', qdbc.Int(), False),
        ('col_long', qdbc.Long(), False),
        ('col_float', qdbc.Float(), False),
        ('col_double', qdbc.Double(), False),
        ('col_symbol', qdbc.Symbol(), False),
        ('col_string', qdbc.String(), False),
        ('col_char', qdbc.Char(), False),
        ('col_uuid', qdbc.UUID(), False),
        ('col_date', qdbc.Date(), False),
        ('col_ts', qdbc.Timestamp(), True),
        ('col_geohash', qdbc.GeohashInt(), False),
        ('col_long256', qdbc.Long256(), False)
    ])


def test_inspect_2(test_engine, test_metrics):
    metadata = sqla.MetaData()
    table = sqla.Table(METRICS_TABLE_NAME, metadata, autoload_with=test_engine)
    table_columns = str([(col.name, col.type, col.primary_key) for col in table.columns])
    assert table_columns == str([
        ('source', qdbc.Symbol(), False),
        ('attr_name', qdbc.Symbol(), False),
        ('attr_value', qdbc.Double(), False),
        ('ts', qdbc.Timestamp(), True),
    ])


def test_multiple_insert(test_engine, test_model):
    now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
    now_date = now.date()
    session = Session(test_engine)
    num_rows = 3
    expected = ("(True, 8, 12, 2, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')\n"
                "(True, 8, 12, 1, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')\n"
                "(True, 8, 12, 0, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')")
    try:
        for idx in range(num_rows):
            session.add(test_model(
                col_boolean=True,
                col_byte=8,
                col_short=12,
                col_int=idx,
                col_long=14,
                col_float=15.234,
                col_double=16.88993244,
                col_symbol='coconut',
                col_string='banana',
                col_char='C',
                col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
                col_date=now_date,
                col_ts=now,
                col_geohash='dfvgsj2vptwu',
                col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a'
            ))
            session.commit()
        assert collect_select_all(session, expected_rows=num_rows) == expected
    finally:
        if session:
            session.close()
        assert collect_select_all_raw_connection(test_engine, expected_rows=num_rows) == expected


def test_bulk_insert(test_engine, test_model):
    now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
    now_date = now.date()
    session = Session(test_engine)
    num_rows = 3
    expected = ("(True, 8, 12, 2, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')\n"
                "(True, 8, 12, 1, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')\n"
                "(True, 8, 12, 0, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a')")
    models = [
        test_model(
            col_boolean=True,
            col_byte=8,
            col_short=12,
            col_int=idx,
            col_long=14,
            col_float=15.234,
            col_double=16.88993244,
            col_symbol='coconut',
            col_string='banana',
            col_char='C',
            col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
            col_date=now_date,
            col_ts=now,
            col_geohash='dfvgsj2vptwu',
            col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a'
        ) for idx in range(num_rows)
    ]
    try:
        session.bulk_save_objects(models)
        session.commit()
        assert collect_select_all(session, expected_rows=num_rows) == expected
    finally:
        if session:
            session.close()
        assert collect_select_all_raw_connection(test_engine, expected_rows=num_rows) == expected


def test_functions(test_engine):
    with test_engine.connect() as conn:
        expected = [row[0] for row in conn.execute("SELECT name FROM functions()").fetchall()]
        assert qdbc.get_functions_list() == expected


def test_keywords(test_engine):
    with test_engine.connect() as conn:
        expected = [row[0] for row in conn.execute("SELECT keyword FROM keywords()").fetchall()]
        assert qdbc.get_keywords_list() == expected
