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
import datetime
import json
import os
import time

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

import questdb_connect.dialect as qdbc
from sqlalchemy import Column, MetaData, insert, text
from sqlalchemy.orm import declarative_base

from examples import CONNECTION_ATTRS

Base = declarative_base(metadata=MetaData())
table_name = 'all_types'


class MyTable(Base):
    __tablename__ = table_name
    __table_args__ = (
        qdbc.QDBTableEngine(table_name, 'col_ts', qdbc.PartitionBy.DAY, is_wal=True),)
    col_boolean = Column(qdbc.Boolean)
    col_byte = Column(qdbc.Byte)
    col_short = Column(qdbc.Short)
    col_int = Column(qdbc.Int)
    col_long = Column(qdbc.Long)
    col_float = Column(qdbc.Float)
    col_double = Column(qdbc.Double)
    col_symbol = Column(qdbc.Symbol)
    col_string = Column(qdbc.String)
    col_char = Column(qdbc.Char)
    col_uuid = Column(qdbc.UUID)
    col_date = Column(qdbc.Date)
    col_ts = Column(qdbc.Timestamp, primary_key=True)
    col_geohash = Column(qdbc.GeohashInt)
    col_long256 = Column(qdbc.Long256)


def main():
    # obtain the engine, which we will dispose of at the end in the finally
    engine = qdbc.create_engine(**CONNECTION_ATTRS)
    try:
        # delete any previous existing 'all_types' table
        while True:
            try:
                Base.metadata.drop_all(engine)
                break
            except Exception as see:
                if "Connection refused" in str(see.orig):
                    print(f"awaiting for QuestDB to start")
                    time.sleep(3)
                else:
                    raise see

        # create the 'all_types' table
        Base.metadata.create_all(engine)

        # connect with QuestDB
        with engine.connect() as conn:
            # insert a fully populated row
            now = datetime.datetime(2023, 4, 22, 18, 10, 10, 765123)
            conn.execute(insert(MyTable).values(
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
                col_date=now.date(),
                col_ts=now,
                col_geohash='dfvgsj2vptwu',
                col_long256='0xa3b400fcf6ed707d710d5d4e672305203ed3cc6254d1cefe313e4a465861f42a'
            ))
            columns = [col.name for col in MyTable.__table__.columns]
            while True:
                rs = conn.execute(text('all_types'))
                if rs.rowcount:
                    print(f'rows: {rs.rowcount}')
                    for row in rs:
                        print(json.dumps(dict(zip(columns, map(str, row))), indent=4))
                    break
    finally:
        if engine:
            engine.dispose()


if __name__ == '__main__':
    main()
