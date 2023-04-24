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
from typing import Dict, Optional

from superset.db_engine_specs.base import BaseEngineSpec, BasicParametersMixin, BasicParametersType
from superset.utils.core import GenericDataType

import questdb_connect as qdbc
from questdb_connect.dialect import connection_uri

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
            qdbc.Boolean(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^byte", re.IGNORECASE),
            qdbc.Byte(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^short", re.IGNORECASE),
            qdbc.Short(),
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile(r"^smallint", re.IGNORECASE),
            qdbc.Short(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^smallserial", re.IGNORECASE),
            qdbc.Short(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^int(eger)?", re.IGNORECASE),
            qdbc.Integer(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^serial", re.IGNORECASE),
            qdbc.Integer(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^bigint", re.IGNORECASE),
            qdbc.Long(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^long", re.IGNORECASE),
            qdbc.Long(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^bigserial", re.IGNORECASE),
            qdbc.Long(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^float", re.IGNORECASE),
            qdbc.Float(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^double", re.IGNORECASE),
            qdbc.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^decimal", re.IGNORECASE),
            qdbc.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^numeric", re.IGNORECASE),
            qdbc.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^real", re.IGNORECASE),
            qdbc.Double(),
            GenericDataType.NUMERIC,
        ),
        (
            re.compile(r"^symbol", re.IGNORECASE),
            qdbc.Symbol(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^string", re.IGNORECASE),
            qdbc.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^long256", re.IGNORECASE),
            qdbc.Long256(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^geohash\((\d+)([b|c])\)", re.IGNORECASE),
            qdbc.geohash_type(60),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^uuid", re.IGNORECASE),
            qdbc.UUID(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"varchar", re.IGNORECASE),
            qdbc.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^(tiny|medium|long)?text", re.IGNORECASE),
            qdbc.String(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^char", re.IGNORECASE),
            qdbc.Char(),
            GenericDataType.STRING,
        ),
        (
            re.compile(r"^timestamp", re.IGNORECASE),
            qdbc.Timestamp(),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^datetime", re.IGNORECASE),
            qdbc.Timestamp(),
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile(r"^date", re.IGNORECASE),
            qdbc.Date(),
            GenericDataType.TEMPORAL,
        ),
    )

    @classmethod
    def build_sqlalchemy_uri(
            cls,
            parameters: BasicParametersType,
            encrypted_extra: Optional[Dict[str, str]] = None
    ) -> str:
        return connection_uri(
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
