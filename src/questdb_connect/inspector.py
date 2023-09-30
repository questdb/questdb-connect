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
import abc

import sqlalchemy

from .common import PartitionBy
from .table_engine import QDBTableEngine
from .types import resolve_type_from_name


class QDBInspector(sqlalchemy.engine.reflection.Inspector, abc.ABC):
    def reflecttable(
        self,
        table,
        include_columns,
        exclude_columns=(),
        resolve_fks=True,
        _extend_on=None,
    ):
        # backward compatibility SQLAlchemy 1.3
        return self.reflect_table(
            table, include_columns, exclude_columns, resolve_fks, _extend_on
        )

    def reflect_table(
        self,
        table,
        include_columns=None,
        exclude_columns=None,
        resolve_fks=False,
        _extend_on=None,
    ):
        table_name = table.name
        result_set = self.bind.execute(f"tables() WHERE name = '{table_name}'")
        if not result_set:
            self._panic_table(table_name)
        table_attrs = result_set.first()
        if table_attrs:
            col_ts_name = table_attrs["designatedTimestamp"]
            partition_by = PartitionBy[table_attrs["partitionBy"]]
            is_wal = True if table_attrs["walEnabled"] else False
        else:
            col_ts_name = None
            partition_by = PartitionBy.NONE
            is_wal = True
        dedup_upsert_keys = []
        for row in self.bind.execute(f"table_columns('{table_name}')"):
            col_name = row[0]
            if include_columns and col_name not in include_columns:
                continue
            if exclude_columns and col_name in exclude_columns:
                continue
            if row[6]:  # upsertKey
                dedup_upsert_keys.append(col_name)
            col_type = resolve_type_from_name(row[1])
            if col_ts_name and col_ts_name.upper() == col_name.upper():
                table.append_column(
                    sqlalchemy.Column(col_name, col_type, primary_key=True)
                )
            else:
                table.append_column(sqlalchemy.Column(col_name, col_type))
        table.engine = QDBTableEngine(
            table_name,
            col_ts_name,
            partition_by,
            is_wal,
            tuple(dedup_upsert_keys) if dedup_upsert_keys else None,
        )
        table.metadata = sqlalchemy.MetaData()

    def get_columns(self, table_name, schema=None, **kw):
        result_set = self.bind.execute(f"table_columns('{table_name}')")
        return self.format_table_columns(table_name, result_set)

    def get_schema_names(self):
        return ["public"]

    def format_table_columns(self, table_name, result_set):
        if not result_set:
            self._panic_table(table_name)
        return [
            {
                "name": row[0],
                "type": resolve_type_from_name(row[1])(),
                "nullable": True,
                "autoincrement": False,
            }
            for row in result_set
        ]

    def _panic_table(self, table_name):
        raise sqlalchemy.orm.exc.NoResultFound(f"Table '{table_name}' does not exist")
