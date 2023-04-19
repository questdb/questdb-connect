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
import json
import os

import questdb_connect


def print_partition(row):
    p_index, p_by, _, min_ts, max_ts, num_rows, _, p_size, *_ = row
    print(f' - Partition {p_index} by {p_by} [{min_ts}, {max_ts}] {num_rows} rows {p_size}')


def print_table(row):
    table_id, table_name, ts_column, p_by, _, _, is_wal, dir_name = row
    msg = f'Table id:{table_id},'
    msg += f' name:{table_name},'
    msg += f' ts-col:{ts_column},'
    msg += f' partition-by:{p_by},'
    msg += f' is-wal:{is_wal},'
    msg += f' dir-name:{dir_name}'
    print(msg)


if __name__ == '__main__':

    connection_attrs = {
        'host': os.environ.get('QUESTDB_CONNECT_HOST', 'localhost'),
        'port': int(os.environ.get('QUESTDB_CONNECT_PORT', '8812')),
        'username': os.environ.get('QUESTDB_CONNECT_USER', 'admin'),
        'password': os.environ.get('QUESTDB_CONNECT_PASSWORD', 'quest')
    }
    with questdb_connect.connect(**connection_attrs) as conn:
        print(f'QuestDB server information: {json.dumps(conn.get_dsn_parameters(), indent=4)}')
        with conn.cursor() as cur0, conn.cursor() as cur1:
            cur0.execute("tables()")
            for rec0 in cur0.fetchall():
                print_table(rec0)
                cur1.execute(f"table_partitions('{rec0[1]}')")
                for rec1 in cur1.fetchall():
                    print_partition(rec1)
