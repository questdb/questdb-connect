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
import psycopg2

# ===== DBAPI =====

apilevel = '2.0'
threadsafety = 2
paramstyle = 'pyformat'


def connect(**kwargs):
    host = kwargs.get('host') or '127.0.0.1'
    port = kwargs.get('port') or 8812
    user = kwargs.get('username') or 'admin'
    passwd = kwargs.get('password') or 'quest'
    return psycopg2.connect(host=host, port=port, user=user, password=passwd)
