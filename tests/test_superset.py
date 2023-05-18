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
from unittest import mock

import pytest
import sqlalchemy as sqla
from questdb_connect import types
from questdb_connect.dialect import QuestDBDialect
from questdb_connect.superset_engine import QDBEngineSpec


def test_build_sqlalchemy_uri():
    request_uri = QDBEngineSpec.build_sqlalchemy_uri({
        'host': 'localhost',
        'port': 8812,
        'username': 'admin',
        'password': 'quest',
        'database': 'main',
    })
    assert 'questdb://admin:quest@localhost:8812/main' == request_uri


@pytest.mark.parametrize(
    ('target_type', 'expected_result', 'dttm'),
    [
        (
                "Date",
                "TO_DATE('2023-04-28', 'YYYY-MM-DD')",
                datetime.datetime(2023, 4, 28, 23, 55, 59, 281567)
        ),
        (
                "DateTime",
                "TO_TIMESTAMP('2023-04-28 23:55:59.281567', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')",
                datetime.datetime(2023, 4, 28, 23, 55, 59, 281567)
        ),
        (
                "TimeStamp",
                "TO_TIMESTAMP('2023-04-28 23:55:59.281567', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')",
                datetime.datetime(2023, 4, 28, 23, 55, 59, 281567)
        ),
        ("UnknownType", None, datetime.datetime(2023, 4, 28, 23, 55, 59, 281567)),
    ],
)
def test_convert_dttm(target_type, expected_result, dttm) -> None:
    # datetime(year, month, day, hour, minute, second, microsecond)
    for target in (
            target_type,
            target_type.upper(),
            target_type.lower(),
            target_type.capitalize(),
    ):
        assert QDBEngineSpec.convert_dttm(target_type=target, dttm=dttm) == expected_result


def test_epoch_to_dttm():
    assert QDBEngineSpec.epoch_to_dttm() == '{col} * 1000000'


def test_get_table_names():
    inspector = mock.Mock()
    inspector.get_table_names = mock.Mock(return_value=['public.table', 'table_2', '"public.table_3"'])
    pg_result = QDBEngineSpec.get_table_names(
        database=mock.ANY, schema="public", inspector=inspector
    )
    assert pg_result == {'table', '"public.table_3"', 'table_2'}


def test_time_exp_literal_no_grain():
    col = sqla.Column('col_ts', types.Timestamp, primary_key=True)
    expr = QDBEngineSpec.get_timestamp_expr(col, None, None)
    assert str(expr.compile(None, dialect=QuestDBDialect())) == 'col_ts'


def test_time_exp_literal_1y_grain():
    col = sqla.Column('col_ts', types.Timestamp, primary_key=True)
    expr = QDBEngineSpec.get_timestamp_expr(col, None, 'P1Y')
    assert str(expr.compile(None, dialect=QuestDBDialect())) == "date_trunc('year', col_ts)"


def test_time_exp_highr():
    col = sqla.Column('col_ts', types.Timestamp, primary_key=True)
    expr = QDBEngineSpec.get_timestamp_expr(col, 'epoch_ms', None)
    assert str(expr.compile(None, dialect=QuestDBDialect())) == '(col_ts/1000) * 1000000'


def test_time_exp_lowr_col_sec_1y():
    col = sqla.Column('col_ts', types.Timestamp, primary_key=True)
    expr = QDBEngineSpec.get_timestamp_expr(col, "epoch_s", "P1Y")
    assert str(expr.compile(None, dialect=QuestDBDialect())) == "date_trunc('year', col_ts * 1000000)"


def test_time_exp_highr_col_micro_1y():
    col = sqla.Column('col_ts', types.Timestamp, primary_key=True)
    expr = QDBEngineSpec.get_timestamp_expr(col, "epoch_ms", "P1Y")
    assert str(expr.compile(None, dialect=QuestDBDialect())) == "date_trunc('year', (col_ts/1000) * 1000000)"

