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
# @Test
# public void testFunctions() throws Exception {
#     assertMemoryLeak(() -> {
#         try (
#             ServerMain qdb = new ServerMain(getServerMainArgs());
#             SqlCompiler compiler = new SqlCompiler(qdb.getEngine())
#         ) {
#             qdb.start();
#             FunctionFactoryCache cache = compiler.getFunctionFactoryCache();
#             cache.getFactories().forEach((key, value) -> {
#                 for (int i = 0, n = value.size(); i < n; i++) {
#                     String name = value.getQuick(i).getName();
#                     if (name.length() > 2) {
#                         System.out.printf("'%s',%n", name);
#                     }
#                     break;
#                 }
#             });
#         }
#     });
# }
#
from sqlalchemy.exc import ArgumentError

FUNCTION_NAMES = [
    'VARCHAR',
    'abs',
    'acos',
    'all_tables',
    'and',
    'asin',
    'atan',
    'atan2',
    'avg',
    'base64',
    'between',
    'build',
    'case',
    'cast',
    'ceil',
    'ceiling',
    'coalesce',
    'concat',
    'cos',
    'cot',
    'count',
    'count_distinct',
    'current_database',
    'current_schema',
    'current_schemas',
    'current_user',
    'date_trunc',
    'dateadd',
    'datediff',
    'day',
    'day_of_week',
    'day_of_week_sunday_first',
    'days_in_month',
    'degrees',
    'dump_memory_usage',
    'dump_thread_stacks',
    'extract',
    'first',
    'floor',
    'flush_query_cache',
    'format_type',
    'haversine_dist_deg',
    'hour',
    'ilike',
    'information_schema._pg_expandarray',
    'isOrdered',
    'is_leap_year',
    'ksum',
    'last',
    'left',
    'length',
    'like',
    'list',
    'log',
    'long_sequence',
    'lower',
    'lpad',
    'ltrim',
    'make_geohash',
    'max',
    'memory_metrics',
    'micros',
    'millis',
    'min',
    'minute',
    'month',
    'not',
    'now',
    'nsum',
    'nullif',
    'pg_advisory_unlock_all',
    'pg_attrdef',
    'pg_attribute',
    'pg_catalog.age',
    'pg_catalog.current_database',
    'pg_catalog.current_schema',
    'pg_catalog.current_schemas',
    'pg_catalog.pg_attrdef',
    'pg_catalog.pg_attribute',
    'pg_catalog.pg_class',
    'pg_catalog.pg_database',
    'pg_catalog.pg_description',
    'pg_catalog.pg_get_expr',
    'pg_catalog.pg_get_keywords',
    'pg_catalog.pg_get_partkeydef',
    'pg_catalog.pg_get_userbyid',
    'pg_catalog.pg_index',
    'pg_catalog.pg_inherits',
    'pg_catalog.pg_is_in_recovery',
    'pg_catalog.pg_locks',
    'pg_catalog.pg_namespace',
    'pg_catalog.pg_roles',
    'pg_catalog.pg_shdescription',
    'pg_catalog.pg_table_is_visible',
    'pg_catalog.pg_type',
    'pg_catalog.txid_current',
    'pg_catalog.version',
    'pg_class',
    'pg_database',
    'pg_description',
    'pg_get_expr',
    'pg_get_keywords',
    'pg_get_partkeydef',
    'pg_index',
    'pg_inherits',
    'pg_is_in_recovery',
    'pg_locks',
    'pg_namespace',
    'pg_postmaster_start_time',
    'pg_proc',
    'pg_range',
    'pg_roles',
    'pg_type',
    'position',
    'power',
    'radians',
    'reader_pool',
    'regexp_replace',
    'replace',
    'right',
    'rnd_bin',
    'rnd_boolean',
    'rnd_byte',
    'rnd_char',
    'rnd_date',
    'rnd_double',
    'rnd_float',
    'rnd_geohash',
    'rnd_int',
    'rnd_log',
    'rnd_long',
    'rnd_long256',
    'rnd_short',
    'rnd_str',
    'rnd_symbol',
    'rnd_timestamp',
    'rnd_uuid4',
    'round',
    'round_down',
    'round_half_even',
    'round_up',
    'row_number',
    'rpad',
    'rtrim',
    'second',
    'session_user',
    'simulate_crash',
    'sin',
    'size_pretty',
    'split_part',
    'sqrt',
    'starts_with',
    'stddev_samp',
    'string_agg',
    'strpos',
    'substring',
    'sum',
    'switch',
    'sysdate',
    'systimestamp',
    'table_columns',
    'table_partitions',
    'table_writer_metrics',
    'tables',
    'tan',
    'timestamp_ceil',
    'timestamp_floor',
    'timestamp_sequence',
    'timestamp_shuffle',
    'to_char',
    'to_date',
    'to_long128',
    'to_lowercase',
    'to_pg_date',
    'to_str',
    'to_timestamp',
    'to_timezone',
    'to_uppercase',
    'to_utc',
    'touch',
    'trim',
    'txid_current',
    'typeOf',
    'upper',
    'version',
    'wal_tables',
    'week_of_year',
    'year',
]

_func_name_set = set(FUNCTION_NAMES)


def is_function_call(token):
    if token:
        open_p = token.index('(') if '(' in token else None
        close_p = token.index(')') if ')' in token else None
        if open_p and close_p:
            fun_name = token[:open_p]
        elif not open_p and not close_p:
            fun_name = token
        else:
            raise ArgumentError(f'bad syntax: {token}')
        return fun_name.lower() in _func_name_set
