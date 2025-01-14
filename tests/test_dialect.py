import datetime

import questdb_connect
import questdb_connect as qdbc
import sqlalchemy as sqla
from sqlalchemy.orm import Session

from tests.conftest import (
    ALL_TYPES_TABLE_NAME,
    METRICS_TABLE_NAME,
    collect_select_all,
    collect_select_all_raw_connection,
    wait_until_table_is_ready,
)


def test_sample_by_in_subquery(test_engine, test_model):
    """Test SAMPLE BY usage within subqueries."""
    base_ts = datetime.datetime(2023, 4, 12, 0, 0, 0)
    session = Session(test_engine)
    try:
        # Insert test data - one row every minute for 2 hours
        num_rows = 120  # 2 hours * 60 minutes
        models = [
            test_model(
                col_boolean=True,
                col_byte=8,
                col_short=12,
                col_int=idx,
                col_long=14,
                col_float=15.234,
                col_double=16.88993244,
                col_symbol='coconut',
                col_string='banana',
                col_char='C',
                col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
                col_date=base_ts.date(),
                col_ts=base_ts + datetime.timedelta(minutes=idx),
                col_geohash='dfvgsj2vptwu',
                col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
                col_varchar='pineapple'
            ) for idx in range(num_rows)
        ]
        session.bulk_save_objects(models)
        session.commit()

        metadata = sqla.MetaData()
        table = sqla.Table(ALL_TYPES_TABLE_NAME, metadata, autoload_with=test_engine)
        wait_until_table_is_ready(test_engine, ALL_TYPES_TABLE_NAME, num_rows)

        with test_engine.connect() as conn:
            # Subquery with SAMPLE BY
            subq = (
                questdb_connect.select(
                    table.c.col_ts,
                    sqla.func.avg(table.c.col_int).label('avg_int')
                )
                .sample_by(30, 'm')  # 30 minute samples in subquery
                .subquery()
            )

            # Main query selecting from subquery with extra conditions
            query = (
                questdb_connect.select(
                    subq.c.col_ts,
                    subq.c.avg_int
                )
                .where(subq.c.avg_int > 30)
                .order_by(subq.c.col_ts)
            )

            result = conn.execute(query)
            rows = result.fetchall()

            # Should only get samples from second half of the data
            # where averages are > 30
            assert len(rows) == 3  # expecting 2 30-min samples > 30
            assert all(row.avg_int > 30 for row in rows)
            assert rows[0].avg_int < rows[1].avg_int  # ordered by timestamp

            # Test nested aggregation
            outer_query = (
                questdb_connect.select(
                    sqla.func.sum(subq.c.avg_int).label('total_avg')
                )
                .select_from(subq)
            )

            result = conn.execute(outer_query)
            row = result.fetchone()
            # Sum of all 30-min sample averages
            assert row.total_avg == 238;

    finally:
        if session:
            session.close()

def test_sample_by_clause(test_engine, test_model):
    """Test SAMPLE BY clause functionality."""
    base_ts = datetime.datetime(2023, 4, 12, 0, 0, 0)
    session = Session(test_engine)
    try:
        # Insert test data - one row every minute for 2 hours
        num_rows = 120  # 2 hours * 60 minutes
        models = [
            test_model(
                col_boolean=True,
                col_byte=8,
                col_short=12,
                col_int=idx,
                col_long=14,
                col_float=15.234,
                col_double=16.88993244,
                col_symbol='coconut',
                col_string='banana',
                col_char='C',
                col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
                col_date=base_ts.date(),
                # Add idx minutes to base timestamp
                col_ts=base_ts + datetime.timedelta(minutes=idx),
                col_geohash='dfvgsj2vptwu',
                col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
                col_varchar='pineapple'
            ) for idx in range(num_rows)
        ]
        session.bulk_save_objects(models)
        session.commit()

        metadata = sqla.MetaData()
        table = sqla.Table(ALL_TYPES_TABLE_NAME, metadata, autoload_with=test_engine)
        wait_until_table_is_ready(test_engine, ALL_TYPES_TABLE_NAME, num_rows)

        with test_engine.connect() as conn:
            # Simple SAMPLE BY
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(30, 'm')  # 30 minute samples
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 4  # 2 hours should give us 4 30-minute samples

            # Verify sample averages
            # First 30 min should average 0-29, second 30-59, etc.
            expected_averages = [14.5, 44.5, 74.5, 104.5]  # (min+max)/2 for each 30-min period
            for row, expected_avg in zip(rows, expected_averages):
                assert abs(row.avg_int - expected_avg) < 0.1

            # SAMPLE BY with ORDER BY
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(1, 'h')  # 1 hour samples
                .order_by(sqla.desc('avg_int'))
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 2  # 2 one-hour samples
            assert rows[0].avg_int > rows[1].avg_int  # Descending order

            # SAMPLE BY with WHERE clause
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .where(table.c.col_int > 30)
                .sample_by(1, 'h')
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 2
            assert all(row.avg_int > 30 for row in rows)

            # SAMPLE BY with LIMIT
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(15, 'm')  # 15 minute samples
                .limit(3)
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 3  # Should limit to first 3 samples

            # SAMPLE BY with GROUP BY, ORDER BY and LIMIT
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(30, 'm')  # 15 minute samples
                .group_by(table.c.col_int)
                .order_by(table.c.col_int.desc())
                .limit(3)
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 3  # Should limit to first 3 samples
            # expected results:
            # 2023-04-12T01:30:00.000000Z, 119
            # 2023-04-12T01:30:00.000000Z, 118
            # 2023-04-12T01:30:00.000000Z, 117
            assert rows[0].col_ts == datetime.datetime(2023, 4, 12, 1, 30)
            assert rows[0].avg_int == 119

            assert rows[1].col_ts == datetime.datetime(2023, 4, 12, 1, 30)
            assert rows[1].avg_int == 118

            assert rows[2].col_ts == datetime.datetime(2023, 4, 12, 1, 30)
            assert rows[2].avg_int == 117
    finally:
        if session:
            session.close()

def test_insert(test_engine, test_model):
    with test_engine.connect() as conn:
        assert test_engine.dialect.has_table(conn, ALL_TYPES_TABLE_NAME)
        assert not test_engine.dialect.has_table(conn, 'scorchio')
        now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
        now_date = now.date()
        expected = ("(True, 8, 12, 13, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                    "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                    "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                    "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')\n"
                    "(True, 8, 12, 13, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                    "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                    "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                    "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')")
        insert_stmt = sqla.insert(test_model).values(
            col_boolean=True,
            col_byte=8,
            col_short=12,
            col_int=13,
            col_long=14,
            col_float=15.234,
            col_double=16.88993244,
            col_symbol='coconut',
            col_string='banana',
            col_char='C',
            col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
            col_date=now_date,
            col_ts=now,
            col_geohash='dfvgsj2vptwu',
            col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
            col_varchar='pineapple'
        )
        conn.execute(insert_stmt)
        conn.execute(sqla.insert(test_model), {
            'col_boolean': True,
            'col_byte': 8,
            'col_short': 12,
            'col_int': 13,
            'col_long': 14,
            'col_float': 15.234,
            'col_double': 16.88993244,
            'col_symbol': 'coconut',
            'col_string': 'banana',
            'col_char': 'C',
            'col_uuid': '6d5eb038-63d1-4971-8484-30c16e13de5b',
            'col_date': now_date,
            'col_ts': now,
            'col_geohash': 'dfvgsj2vptwu',
            'col_long256': '0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
            'col_varchar': 'pineapple'
        })
        assert collect_select_all(conn, expected_rows=2) == expected
    assert collect_select_all_raw_connection(test_engine, expected_rows=2) == expected


def test_inspect_1(test_engine, test_model):
    now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
    now_date = now.date()
    session = Session(test_engine)
    try:
        session.add(test_model(
            col_boolean=True,
            col_byte=8,
            col_short=12,
            col_int=0,
            col_long=14,
            col_float=15.234,
            col_double=16.88993244,
            col_symbol='coconut',
            col_string='banana',
            col_char='C',
            col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
            col_date=now_date,
            col_ts=now,
            col_geohash='dfvgsj2vptwu',
            col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
            col_varchar='pineapple'
        ))
        session.commit()
    finally:
        if session:
            session.close()
    metadata = sqla.MetaData()
    table = sqla.Table(ALL_TYPES_TABLE_NAME, metadata, autoload_with=test_engine)
    table_columns = str([(col.name, col.type, col.primary_key) for col in table.columns])
    assert table_columns == str([
        ('col_boolean', qdbc.Boolean(), False),
        ('col_byte', qdbc.Byte(), False),
        ('col_short', qdbc.Short(), False),
        ('col_int', qdbc.Int(), False),
        ('col_long', qdbc.Long(), False),
        ('col_float', qdbc.Float(), False),
        ('col_double', qdbc.Double(), False),
        ('col_symbol', qdbc.Symbol(), False),
        ('col_string', qdbc.String(), False),
        ('col_char', qdbc.Char(), False),
        ('col_uuid', qdbc.UUID(), False),
        ('col_date', qdbc.Date(), False),
        ('col_ts', qdbc.Timestamp(), True),
        ('col_geohash', qdbc.GeohashInt(), False),
        ('col_long256', qdbc.Long256(), False),
        ('col_varchar', qdbc.Varchar(), False),
    ])


def test_inspect_2(test_engine, test_metrics):
    metadata = sqla.MetaData()
    table = sqla.Table(METRICS_TABLE_NAME, metadata, autoload_with=test_engine)
    table_columns = str([(col.name, col.type, col.primary_key) for col in table.columns])
    assert table_columns == str([
        ('source', qdbc.Symbol(), False),
        ('attr_name', qdbc.Symbol(), False),
        ('attr_value', qdbc.Double(), False),
        ('ts', qdbc.Timestamp(), True),
    ])


def test_multiple_insert(test_engine, test_model):
    now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
    now_date = now.date()
    session = Session(test_engine)
    num_rows = 3
    expected = ("(True, 8, 12, 0, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')\n"
                "(True, 8, 12, 1, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')\n"
                "(True, 8, 12, 2, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')")
    try:
        for idx in range(num_rows):
            session.add(test_model(
                col_boolean=True,
                col_byte=8,
                col_short=12,
                col_int=idx,
                col_long=14,
                col_float=15.234,
                col_double=16.88993244,
                col_symbol='coconut',
                col_string='banana',
                col_char='C',
                col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
                col_date=now_date,
                col_ts=now,
                col_geohash='dfvgsj2vptwu',
                col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
                col_varchar='pineapple'
            ))
            session.commit()
        assert collect_select_all(session, expected_rows=num_rows) == expected
    finally:
        if session:
            session.close()
        assert collect_select_all_raw_connection(test_engine, expected_rows=num_rows) == expected


def test_bulk_insert(test_engine, test_model):
    now = datetime.datetime(2023, 4, 12, 23, 55, 59, 342380)
    now_date = now.date()
    session = Session(test_engine)
    num_rows = 3
    expected = ("(True, 8, 12, 0, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')\n"
                "(True, 8, 12, 1, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')\n"
                "(True, 8, 12, 2, 14, 15.234, 16.88993244, 'coconut', 'banana', 'C', "
                "UUID('6d5eb038-63d1-4971-8484-30c16e13de5b'), datetime.datetime(2023, 4, 12, "
                "0, 0), datetime.datetime(2023, 4, 12, 23, 55, 59, 342380), 'dfvgsj', "
                "'0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a', 'pineapple')")
    models = [
        test_model(
            col_boolean=True,
            col_byte=8,
            col_short=12,
            col_int=idx,
            col_long=14,
            col_float=15.234,
            col_double=16.88993244,
            col_symbol='coconut',
            col_string='banana',
            col_char='C',
            col_uuid='6d5eb038-63d1-4971-8484-30c16e13de5b',
            col_date=now_date,
            col_ts=now,
            col_geohash='dfvgsj2vptwu',
            col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a',
            col_varchar='pineapple'
        ) for idx in range(num_rows)
    ]
    try:
        session.bulk_save_objects(models)
        session.commit()
        assert collect_select_all(session, expected_rows=num_rows) == expected
    finally:
        if session:
            session.close()
        assert collect_select_all_raw_connection(test_engine, expected_rows=num_rows) == expected


def test_sample_by_from_to(test_engine, test_model):
    """Test SAMPLE BY with FROM-TO extension."""
    base_ts = datetime.datetime(2023, 4, 12, 0, 0, 0)
    day_before = base_ts - datetime.timedelta(days=1)
    day_after = base_ts + datetime.timedelta(days=1)
    session = Session(test_engine)
    try:
        num_rows = 6 # 6 hours only
        models = [
            test_model(
                col_int=idx,
                col_ts=base_ts + datetime.timedelta(hours=idx),
            ) for idx in range(num_rows)
        ]

        session.bulk_save_objects(models)
        session.commit()

        metadata = sqla.MetaData()
        table = sqla.Table(ALL_TYPES_TABLE_NAME, metadata, autoload_with=test_engine)
        wait_until_table_is_ready(test_engine, ALL_TYPES_TABLE_NAME, len(models))

        with test_engine.connect() as conn:
            # Test FROM-TO with FILL
            query = (
                questdb_connect.select(
                    table.c.col_ts,
                    sqla.func.avg(table.c.col_int).label('avg_int')
                )
                .sample_by(
                    1, 'h',
                    fill="NULL",
                    from_timestamp=day_before,  # day before data starts
                    to_timestamp=day_after  # day after data ends
                )
            )
            result = conn.execute(query)
            rows = result.fetchall()

            assert len(rows) == 48  # 48 hours in total

            # First rows should be NULL (before our data starts)
            assert rows[0].avg_int is None
            assert rows[1].avg_int is None
            assert rows[2].avg_int is None
            assert rows[3].avg_int is None

            # Middle rows should have data
            assert any(row.avg_int is not None for row in rows[4:-4])

            # Last rows should be NULL (after our data ends)
            assert rows[-4].avg_int is None
            assert rows[-3].avg_int is None
            assert rows[-2].avg_int is None
            assert rows[-1].avg_int is None

            # Test FROM only
            query = (
                questdb_connect.select(
                    table.c.col_ts,
                    sqla.func.avg(table.c.col_int).label('avg_int')
                )
                .sample_by(
                    1, 'h',
                    fill="NULL",
                    from_timestamp=day_before  # day before data starts
                )
            )
            result = conn.execute(query)
            rows = result.fetchall()

            # First rows should be NULL
            assert rows[0].avg_int is None
            assert rows[1].avg_int is None
            assert rows[2].avg_int is None
            assert rows[3].avg_int is None

            # Test TO only
            query = (
                questdb_connect.select(
                    table.c.col_ts,
                    sqla.func.avg(table.c.col_int).label('avg_int')
                )
                .sample_by(
                    1, 'h',
                    fill="NULL",
                    to_timestamp=day_after  # day after data ends
                )
            )
            result = conn.execute(query)
            rows = result.fetchall()

            # Last rows should be NULL
            assert rows[-4].avg_int is None
            assert rows[-3].avg_int is None
            assert rows[-2].avg_int is None
            assert rows[-1].avg_int is None

    finally:
        if session:
            session.close()

def test_sample_by_options(test_engine, test_model):
    """Test SAMPLE BY with ALIGN TO and FILL options."""
    base_ts = datetime.datetime(2023, 4, 12, 0, 0, 0)
    session = Session(test_engine)
    try:
        # Insert test data - one row every hour for a day
        num_rows = 24
        models = [
            test_model(
                col_int=idx,
                col_ts=base_ts + datetime.timedelta(hours=idx),
            ) for idx in range(num_rows)
        ]
        # Add some gaps by removing every 3rd record
        models = [m for i, m in enumerate(models) if i % 3 != 0]

        session.bulk_save_objects(models)
        session.commit()

        metadata = sqla.MetaData()
        table = sqla.Table(ALL_TYPES_TABLE_NAME, metadata, autoload_with=test_engine)
        wait_until_table_is_ready(test_engine, ALL_TYPES_TABLE_NAME, len(models))

        with test_engine.connect() as conn:
            # Test FILL(NULL)
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(15, 'm', fill="NULL")
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert len(rows) == 89
            # Should have NULLs for missing data points
            assert any(row.avg_int is None for row in rows)


            # Test FILL(PREV)
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(15, 'm', fill="PREV")
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert all(row.avg_int is not None for row in rows)

            # Test FILL with constant
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(15, 'm', fill=999.99)
                .limit(10)
            )
            result = conn.execute(query)
            rows = result.fetchall()
            assert any(row.avg_int == 999.99 for row in rows)

            # Test ALIGN TO FIRST OBSERVATION
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(15, 'm', align_to="FIRST OBSERVATION")
                .limit(10)
            )
            result = conn.execute(query)
            first_row = result.fetchone()
            # First timestamp should match our first data point
            assert first_row.col_ts == models[0].col_ts

            # Test with timezone
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(1, 'd', align_to="CALENDAR", timezone="Europe/Prague")
            )
            result = conn.execute(query)
            rows = result.fetchall()
            # First row should be at midnight Prague time, that is 22:00 UTC the previous day
            assert rows[0].col_ts.hour == 22
            assert rows[1].col_ts.hour == 22

            # Test with offset
            query = (
                questdb_connect.select(table.c.col_ts, sqla.func.avg(table.c.col_int).label('avg_int'))
                .sample_by(1, 'd', align_to="CALENDAR", offset="02:00")
            )
            result = conn.execute(query)
            rows = result.fetchall()
            # First row should start at 02:00
            assert rows[0].col_ts.hour == 2

    finally:
        if session:
            session.close()

def test_dialect_get_schema_names(test_engine):
    dialect = qdbc.QuestDBDialect()
    with test_engine.connect() as conn:
        assert dialect.get_schema_names(conn) == ["public"]


def test_dialect_get_table_names(test_engine):
    dialect = qdbc.QuestDBDialect()
    with test_engine.connect() as conn:
        table_names = dialect.get_table_names(conn, schema="public")
        assert table_names == dialect.get_table_names(conn)
        assert len(table_names) > 0


def test_dialect_has_table(test_engine):
    dialect = qdbc.QuestDBDialect()
    with test_engine.connect() as conn:
        for table_name in dialect.get_table_names(conn):
            if not dialect.has_table(conn, table_name):
                raise AssertionError()
            if not dialect.has_table(conn, table_name, schema="public"):
                raise AssertionError()


def test_functions(test_engine):
    with test_engine.connect() as conn:
        sql = sqla.text("SELECT name FROM functions()")
        expected = [row[0] for row in conn.execute(sql).fetchall()]
        assert qdbc.get_functions_list() == expected


def test_keywords(test_engine):
    with test_engine.connect() as conn:
        sql = sqla.text("SELECT keyword FROM keywords()")
        expected = [row[0] for row in conn.execute(sql).fetchall()]
        assert qdbc.get_keywords_list() == expected
