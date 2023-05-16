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
import sqlalchemy as sqla

from tests.conftest import METRICS_TABLE_NAME


def test_compile_select(test_engine, test_metrics):
    select_stmt = 'SELECT ts AS __timestamp,'
    select_stmt += '    attr_name AS attr_name,'
    select_stmt += '    max(attr_value) AS "MAX(attr_value)",'
    select_stmt += '    AVG(attr_value) AS "AVG(attr_value)",'
    select_stmt += '    sum(attr_value) AS "SUM(attr_value)"'
    select_stmt += "  FROM '" + METRICS_TABLE_NAME + "'"
    select_stmt += " WHERE ts >= '2023-05-08 00:00:00.000000'"
    select_stmt += "   AND ts < '2023-05-15 00:00:00.000000'"
    select_stmt += ' GROUP BY attr_name,'
    select_stmt += '          ts'
    select_stmt += '  ORDER BY "MAX(attr_value)" DESC'
    select_stmt += '  LIMIT 10000'
    # session = sqla.orm.Session(test_engine)
    # try:
    #     rs = session.execute(select_stmt)
    #     print()
    # finally:
    #     if session:
    #         session.close()


