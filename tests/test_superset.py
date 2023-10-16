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
from qdb_superset.db_engine_specs.questdb import QuestDbEngineSpec
from questdb_connect.types import QUESTDB_TYPES, Timestamp
from sqlalchemy import column, literal_column
from sqlalchemy.types import TypeEngine


def test_build_sqlalchemy_uri():
    request_uri = QuestDbEngineSpec.build_sqlalchemy_uri(
        {
            "host": "localhost",
            "port": "8812",
            "username": "admin",
            "password": "quest",
            "database": "main",
        }
    )
    assert request_uri == "questdb://admin:quest@localhost:8812/main"


def test_default_schema_for_query():
    assert QuestDbEngineSpec.get_default_schema_for_query("main", None) == None


def test_get_text_clause():
    sql_clause = "SELECT * FROM public.mytable t1"
    sql_clause += " JOIN public.myclient t2 ON t1.id = t2.id"
    expected_clause = "SELECT * FROM mytable t1 JOIN myclient t2 ON t1.id = t2.id"
    actual_clause = str(QuestDbEngineSpec.get_text_clause(sql_clause))
    print(f"sql: {sql_clause}, ex: {expected_clause}, ac: {actual_clause}")
    assert expected_clause == actual_clause


def test_epoch_to_dttm():
    assert QuestDbEngineSpec.epoch_to_dttm() == "{col} * 1000000"


@pytest.mark.parametrize(
    ("target_type", "expected_result", "dttm"),
    [
        (
                "Date",
                "TO_DATE('2023-04-28', 'YYYY-MM-DD')",
                datetime.datetime(2023, 4, 28, 23, 55, 59, 281567),
        ),
        (
                "DateTime",
                "TO_TIMESTAMP('2023-04-28T23:55:59.281567', 'yyyy-MM-ddTHH:mm:ss.SSSUUU')",
                datetime.datetime(2023, 4, 28, 23, 55, 59, 281567),
        ),
        (
                "TimeStamp",
                "TO_TIMESTAMP('2023-04-28T23:55:59.281567', 'yyyy-MM-ddTHH:mm:ss.SSSUUU')",
                datetime.datetime(2023, 4, 28, 23, 55, 59, 281567),
        ),
        ("UnknownType", None, datetime.datetime(2023, 4, 28, 23, 55, 59, 281567)),
    ],
)
def test_convert_dttm(target_type, expected_result, dttm) -> None:
    # datetime(year, month, day, hour, minute, second, microsecond)
    print('sugus')
    for target in (
            target_type,
            target_type.upper(),
            target_type.lower(),
            target_type.capitalize(),
    ):
        assert QuestDbEngineSpec.convert_dttm(
            target_type=target, dttm=dttm
        ) == expected_result


def test_get_datatype():
    assert QuestDbEngineSpec.get_datatype("int") == "INT"
    assert QuestDbEngineSpec.get_datatype(["int"]) == "['int']"


def test_get_column_spec():
    for native_type in QUESTDB_TYPES:
        column_spec = QuestDbEngineSpec.get_column_spec(native_type.__visit_name__)
        assert native_type == column_spec.sqla_type
        assert native_type != Timestamp or column_spec.is_dttm


def test_get_sqla_column_type():
    for native_type in QUESTDB_TYPES:
        column_type = QuestDbEngineSpec.get_sqla_column_type(native_type.__visit_name__)
        assert isinstance(column_type, TypeEngine.__class__)


def test_get_allow_cost_estimate():
    assert not QuestDbEngineSpec.get_allow_cost_estimate(extra=None)


def test_get_view_names():
    assert set() == QuestDbEngineSpec.get_view_names("main", None, None)


def test_get_table_names():
    inspector = mock.Mock()
    inspector.get_table_names = mock.Mock(
        return_value=["public.table", "table_2", '"public.table_3"']
    )
    pg_result = QuestDbEngineSpec.get_table_names(
        database=mock.ANY, schema="public", inspector=inspector
    )
    assert {"table", '"public.table_3"', "table_2"} == pg_result


def test_time_exp_literal_no_grain(test_engine):
    col = literal_column("COALESCE(a, b)")
    expr = QuestDbEngineSpec.get_timestamp_expr(col, None, None)
    result = str(expr.compile(None, dialect=test_engine.dialect))
    assert "COALESCE(a, b)" == result


def test_time_ex_lowr_col_no_grain(test_engine):
    col = column("lower_case")
    expr = QuestDbEngineSpec.get_timestamp_expr(col, None, None)
    result = str(expr.compile(None, dialect=test_engine.dialect))
    assert "lower_case" == result
