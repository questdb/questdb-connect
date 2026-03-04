import abc

import psycopg2
import sqlalchemy
from sqlalchemy.engine import Connection

from .common import PartitionBy
from .table_engine import QDBTableEngine
from .types import resolve_type_from_name


class QDBInspector(sqlalchemy.engine.reflection.Inspector, abc.ABC):
    def _get_connection(self):
        """Get a usable connection from self.bind.

        In SA 1.4, self.bind may be an Engine (with .execute()).
        In SA 2.0, Engine.execute() is removed, so we must use .connect().
        """
        if isinstance(self.bind, Connection):
            return self.bind
        return self.bind.connect()

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
        _reflect_info=None,
    ):
        table_name = table.name
        conn = self._get_connection()
        try:
            try:
                result_set = conn.execute(
                    sqlalchemy.text(
                        "SELECT designatedTimestamp, partitionBy, walEnabled FROM tables() WHERE table_name = :tn"
                    ),
                    {"tn": table_name},
                )
            except psycopg2.DatabaseError:
                # older QuestDB version uses 'name' instead of 'table_name'
                result_set = conn.execute(
                    sqlalchemy.text(
                        "SELECT designatedTimestamp, partitionBy, walEnabled FROM tables() WHERE name = :tn"
                    ),
                    {"tn": table_name},
                )
            if not result_set:
                self._panic_table(table_name)
            table_attrs = result_set.first()
            if table_attrs:
                col_ts_name = table_attrs[0]
                partition_by = PartitionBy[table_attrs[1]]
                is_wal = True if table_attrs[2] else False
            else:
                col_ts_name = None
                partition_by = PartitionBy.NONE
                is_wal = True
            dedup_upsert_keys = []
            for row in conn.execute(
                sqlalchemy.text(
                    'SELECT "column", "type", "upsertKey" FROM table_columns(:tn)'
                ),
                {"tn": table_name},
            ):
                col_name = row[0]
                if include_columns and col_name not in include_columns:
                    continue
                if exclude_columns and col_name in exclude_columns:
                    continue
                if row[2]:  # upsertKey
                    dedup_upsert_keys.append(col_name)
                col_type = resolve_type_from_name(row[1])
                table.append_column(
                    sqlalchemy.Column(
                        col_name,
                        col_type,
                        primary_key=(
                            col_ts_name and col_ts_name.upper() == col_name.upper()
                        ),
                    )
                )
            table.engine = QDBTableEngine(
                table_name,
                col_ts_name,
                partition_by,
                is_wal,
                tuple(dedup_upsert_keys) if dedup_upsert_keys else None,
            )
            table.metadata = sqlalchemy.MetaData()
        finally:
            # Close the connection if we opened it (bind was an Engine)
            if not isinstance(self.bind, Connection):
                conn.close()

    def get_columns(self, table_name, schema=None, **kw):
        conn = self._get_connection()
        try:
            result_set = conn.execute(
                sqlalchemy.text('SELECT "column", "type" FROM table_columns(:tn)'),
                {"tn": table_name},
            )
            return self.format_table_columns(table_name, result_set)
        finally:
            if not isinstance(self.bind, Connection):
                conn.close()

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
