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
import logging
import re
from datetime import datetime
from typing import Dict, Optional, Any

from superset.utils import core as utils
from superset.db_engine_specs.base import BaseEngineSpec, BasicParametersMixin, BasicParametersType
from superset.utils.core import GenericDataType

import questdb_connect.dialect as qdbcd

logger = logging.getLogger(__name__)


# https://superset.apache.org/docs/databases/installing-database-drivers
# Apache Superset requires a Python DB-API database driver, and a SQLAlchemy dialect
# https://preset.io/blog/building-database-connector/


class QDBEngineSpec(BaseEngineSpec, BasicParametersMixin):
    engine = 'questdb'
    engine_name = 'QuestDB Connect'
    default_driver = "psycopg2"
    encryption_parameters = {"sslmode": "require"}
    sqlalchemy_uri_placeholder = "questdb://user:password@host:port/dbname[?key=value&key=value...]"
    time_groupby_inline = True
    time_secondary_columns = True
    max_column_name_length = 120
    _time_grain_expressions = {
        None: '{col}',
        'PT1S': "date_trunc('second', {col})",
        'PT5S': "date_trunc('second', {col}) + 5000000",
        'PT30S': "date_trunc('second', {col}) + 30000000",
        'PT1M': "date_trunc('minute', {col})",
        'PT5M': "date_trunc('minute', {col}) + 300000000",
        'PT10M': "date_trunc('minute', {col}) + 600000000",
        'PT15M': "date_trunc('minute', {col}) + 900000000",
        'PT30M': "date_trunc('minute', {col}) + 1800000000",
        'PT1H': "date_trunc('hour', {col})",
        'PT6H': "date_trunc('hour', {col})",
        'PT1D': "date_trunc('day', {col})",
        'P1W': "date_trunc('week', {col})",
        'P1M': "date_trunc('month', {col})",
        'P1Y': "date_trunc('year', {col})",
        'P3M': "date_trunc('quarter', {col})",
    }
    _default_column_type_mappings = (
        (
            re.compile(r"^bool(ean)?", re.IGNORECASE),
            qdbcd.Boolean(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^byte", re.IGNORECASE),
            qdbcd.Byte(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^short", re.IGNORECASE),
            qdbcd.Short(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^smallint", re.IGNORECASE),
            qdbcd.Short(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^smallserial", re.IGNORECASE),
            qdbcd.Short(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^int(eger)?", re.IGNORECASE),
            qdbcd.Integer(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^serial", re.IGNORECASE),
            qdbcd.Integer(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^bigint", re.IGNORECASE),
            qdbcd.Long(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^long", re.IGNORECASE),
            qdbcd.Long(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^bigserial", re.IGNORECASE),
            qdbcd.Long(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^float", re.IGNORECASE),
            qdbcd.Float(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^double", re.IGNORECASE),
            qdbcd.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^decimal", re.IGNORECASE),
            qdbcd.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^numeric", re.IGNORECASE),
            qdbcd.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^real", re.IGNORECASE),
            qdbcd.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^symbol", re.IGNORECASE),
            qdbcd.Symbol(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^string", re.IGNORECASE),
            qdbcd.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^long256", re.IGNORECASE),
            qdbcd.Long256(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^geohash\((\d+)([b|c])\)", re.IGNORECASE),
            qdbcd.geohash_type(60)(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^uuid", re.IGNORECASE),
            qdbcd.UUID(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"varchar", re.IGNORECASE),
            qdbcd.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^(tiny|medium|long)?text", re.IGNORECASE),
            qdbcd.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^char", re.IGNORECASE),
            qdbcd.Char(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^timestamp", re.IGNORECASE),
            qdbcd.Timestamp(),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^datetime", re.IGNORECASE),
            qdbcd.Timestamp(),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^date", re.IGNORECASE),
            qdbcd.Date(),
            GenericDataType.TEMPORAL,
        ),
    )

    @classmethod
    def build_sqlalchemy_uri(
            cls,
            parameters: BasicParametersType,
            encrypted_extra: Optional[Dict[str, str]] = None
    ) -> str:
        return qdbcd.connection_uri(
            parameters.get("host"),
            int(parameters.get("port")),
            parameters.get("username"),
            parameters.get("password"),
            parameters.get("database"))

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return '{col} * 1000'

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime, *_args, **_kwargs) -> Optional[str]:
        if target_type.upper() == 'DATE':
            return f"TO_DATE('{dttm.date().isoformat()}', 'YYYY-MM-DD')"
        if target_type.upper() == 'DATETIME':
            dttm_formatted = dttm.isoformat(sep=" ", timespec="microseconds")
            return f"TO_TIMESTAMP('{dttm_formatted}', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')"
        return None

    @classmethod
    def get_column_spec(
            cls,
            native_type: Optional[str],
            db_extra: Optional[Dict[str, Any]] = None,
            source: utils.ColumnTypeSource = utils.ColumnTypeSource.GET_TABLE,
    ) -> Optional[utils.ColumnSpec]:
        """Get generic type related specs regarding a native column type.
        :param native_type: Native database type
        :param db_extra: The database extra object
        :param source: Type coming from the database table or cursor description
        :return: ColumnSpec object
        """
        if not native_type:
            return None
        sqla_type = qdbcd.resolve_type_from_name(native_type)
        name_u = sqla_type.__visit_name__.upper()
        generic_type = None
        if name_u == 'BOOLEAN':
            generic_type = GenericDataType.BOOLEAN
        elif name_u in ('BYTE', 'SHORT', 'INT', 'INTEGER', 'LONG', 'FLOAT', 'DOUBLE'):
            generic_type = GenericDataType.NUMERIC
        elif name_u in ('SYMBOL', 'STRING', 'TEXT', 'VARCHAR', 'CHAR', 'LONG256', 'UUID'):
            generic_type = GenericDataType.STRING
        elif name_u in ('DATE', 'TIMESTAMP'):
            generic_type = GenericDataType.TEMPORAL
        elif 'GEOHASH' in name_u and '(' in name_u and ')' in name_u:
            generic_type = GenericDataType.STRING
        return utils.ColumnSpec(sqla_type, generic_type, generic_type == GenericDataType.TEMPORAL)
