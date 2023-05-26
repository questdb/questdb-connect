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
from examples import hello_world, psycopg2_connect, server_utilisation, sqlalchemy_orm, sqlalchemy_raw


def test_hello_world():
    hello_world.main()


def test_psycopg2_connect():
    psycopg2_connect.main()


def test_server_utilisation():
    server_utilisation.main(duration_sec=2.0)


def test_sqlalchemy_orm():
    sqlalchemy_orm.main()


def test_sqlalchemy_raw():
    sqlalchemy_raw.main()
