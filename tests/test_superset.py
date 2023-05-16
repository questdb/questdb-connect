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
from superset.db_engine_specs.base import BasicParametersType, TimeGrain


def test_build_sqlalchemy_uri():
    request_uri = QDBEngineSpec.build_sqlalchemy_uri(BasicParametersType({
        'host': 'localhost',
        'port': 8812,
        'username': 'admin',
        'password': 'quest',
        'database': 'main',
    }))
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


def test_time_grains():
    assert QDBEngineSpec.get_time_grains() == (
        TimeGrain(
            name='Second',
            label='Second',
            function="date_trunc('second', {col})",
            duration='PT1S'),
        TimeGrain(
            name='5 second',
            label='5 second',
            function="date_trunc('second', {col}) + 5000000",
            duration='PT5S'),
        TimeGrain(
            name='30 second',
            label='30 second',
            function="date_trunc('second', {col}) + 30000000",
            duration='PT30S'),
        TimeGrain(
            name='Minute',
            label='Minute',
            function="date_trunc('minute', {col})",
            duration='PT1M'),
        TimeGrain(
            name='5 minute',
            label='5 minute',
            function="date_trunc('minute', {col}) + 300000000",
            duration='PT5M'),
        TimeGrain(
            name='10 minute',
            label='10 minute',
            function="date_trunc('minute', {col}) + 600000000",
            duration='PT10M'),
        TimeGrain(
            name='15 minute',
            label='15 minute',
            function="date_trunc('minute', {col}) + 900000000",
            duration='PT15M'),
        TimeGrain(
            name='30 minute',
            label='30 minute',
            function="date_trunc('minute', {col}) + 1800000000",
            duration='PT30M'),
        TimeGrain(
            name='Hour',
            label='Hour',
            function="date_trunc('hour', {col})",
            duration='PT1H'),
        TimeGrain(
            name='6 hour',
            label='6 hour',
            function="date_trunc('hour', {col})",
            duration='PT6H'),
        TimeGrain(
            name='Week',
            label='Week',
            function="date_trunc('week', {col})",
            duration='P1W'),
        TimeGrain(
            name='Month',
            label='Month',
            function="date_trunc('month', {col})",
            duration='P1M'),
        TimeGrain(
            name='Year',
            label='Year',
            function="date_trunc('year', {col})",
            duration='P1Y'),
        TimeGrain(
            name='Quarter',
            label='Quarter',
            function="date_trunc('quarter', {col})",
            duration='P3M')
    )

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

    def test_parse_sql_removes_timestamp_from_group_by():
        select_stmt = 'SELECT ts AS __timestamp,'
        select_stmt += '    attr_name AS attr_name,'
        select_stmt += '    max(attr_value) AS "MAX(attr_value)",'
        select_stmt += '    AVG(attr_value) AS "AVG(attr_value)",'
        select_stmt += '    sum(attr_value) AS "SUM(attr_value)"'
        select_stmt += '  FROM node_metrics'
        select_stmt += '  JOIN'
        select_stmt += '(SELECT attr_name AS attr_name__,'
        select_stmt += '    max(attr_value) AS mme_inner__'
        select_stmt += '  FROM node_metrics'
        select_stmt += "  WHERE ts >= '2023-05-08 00:00:00.000000'"
        select_stmt += "    AND ts < '2023-05-15 00:00:00.000000'"
        select_stmt += '  GROUP BY attr_name'
        select_stmt += '  ORDER BY mme_inner__ DESC'
        select_stmt += '  LIMIT 500) AS anon_1 ON attr_name = attr_name__'
        select_stmt += " WHERE ts >= '2023-05-08 00:00:00.000000'"
        select_stmt += "   AND ts < '2023-05-15 00:00:00.000000'"
        select_stmt += ' GROUP BY attr_name,'
        select_stmt += '          ts'
        select_stmt += '  ORDER BY "MAX(attr_value)" DESC'
        select_stmt += '  LIMIT 10000'
        assert QDBEngineSpec.parse_sql(select_stmt) == [
            'SELECT ts AS __timestamp,    attr_name AS attr_name,    max(attr_value) AS '
            '"MAX(attr_value)",    AVG(attr_value) AS "AVG(attr_value)",    '
            'sum(attr_value) AS "SUM(attr_value)"  FROM node_metrics  JOIN(SELECT '
            'attr_name AS attr_name__,    max(attr_value) AS mme_inner__  FROM '
            "node_metrics  WHERE ts >= '2023-05-08 00:00:00.000000'    AND ts < "
            "'2023-05-15 00:00:00.000000'  GROUP BY attr_name  ORDER BY mme_inner__ DESC  "
            "LIMIT 500) AS anon_1 ON attr_name = attr_name__ WHERE ts >= '2023-05-08 "
            "00:00:00.000000'   AND ts < '2023-05-15 00:00:00.000000' GROUP BY "
            'attr_name            ORDER BY "MAX(attr_value)" DESC  LIMIT 10000'
        ]
