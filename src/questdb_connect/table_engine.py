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
import typing

import sqlalchemy

from .common import PartitionBy, quote_identifier


class QDBTableEngine(
    sqlalchemy.sql.base.SchemaEventTarget, sqlalchemy.sql.visitors.Traversible
):
    def __init__(
        self,
        table_name: str,
        ts_col_name: str,
        partition_by: PartitionBy = PartitionBy.DAY,
        is_wal: bool = True,
        dedup_upsert_keys: typing.Optional[typing.Tuple[str]] = None,
    ):
        sqlalchemy.sql.visitors.Traversible.__init__(self)
        self.name = table_name
        self.ts_col_name = ts_col_name
        self.partition_by = partition_by
        self.is_wal = is_wal
        self.dedup_upsert_keys = dedup_upsert_keys
        self.compiled = None

    def get_table_suffix(self):
        if self.compiled is None:
            self.compiled = ""
            has_ts = self.ts_col_name is not None
            is_partitioned = self.partition_by and self.partition_by != PartitionBy.NONE
            if has_ts:
                self.compiled += f'TIMESTAMP("{self.ts_col_name}")'
            if is_partitioned:
                if not has_ts:
                    raise sqlalchemy.exc.ArgumentError(
                        None,
                        "Designated timestamp must be specified for partitioned table",
                    )
                self.compiled += f" PARTITION BY {self.partition_by.name}"
            if self.is_wal:
                if not is_partitioned:
                    raise sqlalchemy.exc.ArgumentError(
                        None, "WAL table requires designated timestamp and partition by"
                    )
                if self.is_wal:
                    self.compiled += " WAL"
                    if self.dedup_upsert_keys:
                        self.compiled += " DEDUP UPSERT KEYS("
                        self.compiled += ",".join(
                            map(quote_identifier, self.dedup_upsert_keys)
                        )
                        self.compiled += ")"
                else:
                    if self.dedup_upsert_keys:
                        raise sqlalchemy.exc.ArgumentError(
                            None, "DEDUP only applies to WAL tables"
                        )
                    self.compiled += " BYPASS WAL"
        return self.compiled

    def _set_parent(self, parent, **_kwargs):
        parent.engine = self
