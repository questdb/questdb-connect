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
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask_babel import gettext as __
from marshmallow import Schema, fields
from sqlalchemy.sql import text
from superset.db_engine_specs.base import (
    BaseEngineSpec,
    BasicParametersMixin,
    BasicParametersType,
)
from superset.utils.core import GenericDataType

from . import remove_public_schema, types
from .dialect import connection_uri
from .function_names import FUNCTION_NAMES


# https://superset.apache.org/docs/databases/installing-database-drivers
# Apache Superset requires a Python DB-API database driver, and a SQLAlchemy dialect
# https://preset.io/blog/building-database-connector/


class QDBParametersSchema(Schema):
    username = fields.String(allow_none=True, description=__('user'))
    password = fields.String(allow_none=True, description=__('password'))
    host = fields.String(required=True, description=__('host'))
    port = fields.Integer(allow_none=True, description=__('port'))
    database = fields.String(allow_none=True, description=__('database'))


class QDBEngineSpec(BaseEngineSpec, BasicParametersMixin):
    engine = 'questdb'
    engine_name = 'QuestDB Connect'
    default_driver = "psycopg2"
    encryption_parameters = {"sslmode": "prefer"}
    sqlalchemy_uri_placeholder = "questdb://user:password@host:port/dbname"
    parameters_schema = QDBParametersSchema()
    time_groupby_inline = True
    allows_hidden_cc_in_orderby = True
    time_secondary_columns = True
    try_remove_schema_from_table_name = True
    supports_dynamic_schema = False
    allow_dml = True
    max_column_name_length = 120
    top_keywords = {}

    _time_grain_expressions = {
        None: '{col}',
        "PT1S": "date_trunc('second', {col})",
        "PT5S": "date_trunc('second', {col}) + 5000000L",
        "PT30S": "date_trunc('second', {col}) + 30000000L",
        "PT1M": "date_trunc('minute', {col})",
        "PT5M": "date_trunc('minute', {col}) + 300000000L",
        "PT10M": "date_trunc('minute', {col}) + 600000000L",
        "PT15M": "date_trunc('minute', {col}) + 900000000L",
        "PT30M": "date_trunc('minute', {col}) + 1800000000L",
        "PT1H": "date_trunc('hour', {col})",
        "PT6H": "date_trunc('hour', {col}) + 21600000000L",
        "PT1D": "date_trunc('day', {col})",
        "P1W": "date_trunc('week', {col})",
        "P1M": "date_trunc('month', {col})",
        "P1Y": "date_trunc('year', {col})",
        "P3M": "date_trunc('quarter', {col})"
    }

    column_type_mappings = (
        (re.compile("^LONG256", re.IGNORECASE), types.Long256, GenericDataType.STRING),
        (re.compile("^BOOLEAN", re.IGNORECASE), types.Boolean, GenericDataType.BOOLEAN),
        (re.compile("^BYTE", re.IGNORECASE), types.Byte, GenericDataType.BOOLEAN),
        (re.compile("^SHORT", re.IGNORECASE), types.Short, GenericDataType.NUMERIC),
        (re.compile("^INT", re.IGNORECASE), types.Int, GenericDataType.NUMERIC),
        (re.compile("^LONG", re.IGNORECASE), types.Long, GenericDataType.NUMERIC),
        (re.compile("^FLOAT", re.IGNORECASE), types.Float, GenericDataType.NUMERIC),
        (re.compile("^DOUBLE'", re.IGNORECASE), types.Double, GenericDataType.NUMERIC),
        (re.compile("^SYMBOL", re.IGNORECASE), types.Symbol, GenericDataType.STRING),
        (re.compile("^STRING", re.IGNORECASE), types.String, GenericDataType.STRING),
        (re.compile("^UUID", re.IGNORECASE), types.UUID, GenericDataType.STRING),
        (re.compile("^CHAR", re.IGNORECASE), types.Char, GenericDataType.STRING),
        (re.compile("^TIMESTAMP", re.IGNORECASE), types.Timestamp, GenericDataType.TEMPORAL),
        (re.compile("^DATE", re.IGNORECASE), types.Date, GenericDataType.TEMPORAL),
        (re.compile(r"^GEOHASH\(\d+[b|c]\)", re.IGNORECASE), types.GeohashLong, GenericDataType.STRING)
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
    def get_default_schema_for_query(cls, database, query) -> Optional[str]:
        return None

    @classmethod
    def get_allow_cost_estimate(cls, extra: Dict[str, Any]) -> bool:
        return False

    @classmethod
    def get_view_names(cls, database, inspector, schema: Optional[str]):
        return []

    @classmethod
    def get_text_clause(cls, clause):
        """SQLAlchemy wrapper to ensure text clauses are escaped properly
        :param clause: string clause with potentially unescaped characters
        :return: text clause with escaped characters
        """
        if cls.allows_escaped_colons:
            clause = clause.replace(":", "\\:")
        return text(remove_public_schema(clause))

    @classmethod
    def epoch_to_dttm(cls) -> str:
        """SQL expression that converts epoch (seconds) to datetime that can be used in a
        query. The reference column should be denoted as `{col}` in the return
        expression, e.g. "FROM_UNIXTIME({col})"
        :return: SQL Expression
        """
        return '{col} * 1000000'

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime, *_args, **_kwargs) -> Optional[str]:
        """Convert a Python `datetime` object to a SQL expression.
        :param target_type: The target type of expression
        :param dttm: The datetime object
        :return: The SQL expression
        """
        type_u = target_type.upper()
        if type_u == 'DATE':
            return f"TO_DATE('{dttm.date().isoformat()}', 'YYYY-MM-DD')"
        if type_u in ('DATETIME', 'TIMESTAMP'):
            dttm_formatted = dttm.isoformat(sep=" ", timespec="microseconds")
            return f"TO_TIMESTAMP('{dttm_formatted}', 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ')"
        return None

    @classmethod
    def get_function_names(cls, database) -> List[str]:
        """Get a list of function names that are able to be called on the database.
        Used for SQL Lab autocomplete.
        :param database: The database to get functions for
        :return: A list of function names usable in the database
        """
        return FUNCTION_NAMES
