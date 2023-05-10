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

import pytest
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


def test_get_time_grains():
    assert QDBEngineSpec.get_time_grains() == (
        TimeGrain(name=None, label=None, function='{col}', duration=None),
        TimeGrain(name='PT1S', label='PT1S', function="date_trunc('second', {col})", duration='PT1S'),
        TimeGrain(name='PT5S', label='PT5S', function="date_trunc('second', {col}) + 5000000", duration='PT5S'),
        TimeGrain(name='PT30S', label='PT30S', function="date_trunc('second', {col}) + 30000000", duration='PT30S'),
        TimeGrain(name='PT1M', label='PT1M', function="date_trunc('minute', {col})", duration='PT1M'),
        TimeGrain(name='PT5M', label='PT5M', function="date_trunc('minute', {col}) + 300000000", duration='PT5M'),
        TimeGrain(name='PT10M', label='PT10M', function="date_trunc('minute', {col}) + 600000000", duration='PT10M'),
        TimeGrain(name='PT15M', label='PT15M', function="date_trunc('minute', {col}) + 900000000", duration='PT15M'),
        TimeGrain(name='PT30M', label='PT30M', function="date_trunc('minute', {col}) + 1800000000", duration='PT30M'),
        TimeGrain(name='PT1H', label='PT1H', function="date_trunc('hour', {col})", duration='PT1H'),
        TimeGrain(name='PT6H', label='PT6H', function="date_trunc('hour', {col})", duration='PT6H'),
        TimeGrain(name='PT1D', label='PT1D', function="date_trunc('day', {col})", duration='PT1D'),
        TimeGrain(name='P1W', label='P1W', function="date_trunc('week', {col})", duration='P1W'),
        TimeGrain(name='P1M', label='P1M', function="date_trunc('month', {col})", duration='P1M'),
        TimeGrain(name='P1Y', label='P1Y', function="date_trunc('year', {col})", duration='P1Y'),
        TimeGrain(name='P3M', label='P3M', function="date_trunc('quarter', {col})", duration='P3M')
    )
