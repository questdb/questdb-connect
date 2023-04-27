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
import math
import os
import time

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

import questdb_connect.dialect as qdbcd
import sqlalchemy as sqla
from questdb_connect import types
from sqlalchemy import Column
from sqlalchemy.orm import declarative_base


def main():
    host = os.environ.get('QUESTDB_CONNECT_HOST', 'localhost')
    port = int(os.environ.get('QUESTDB_CONNECT_PORT', '8812'))
    username = os.environ.get('QUESTDB_CONNECT_USER', 'admin')
    password = os.environ.get('QUESTDB_CONNECT_PASSWORD', 'quest')
    database = os.environ.get('QUESTDB_CONNECT_DATABASE', 'main')
    engine = qdbcd.create_engine(host, port, username, password, database)
    try:
        Base = declarative_base(metadata=sqla.MetaData())

        class Trigonometry(Base):
            __tablename__ = 'trigonometry'
            __table_args__ = (qdbcd.QDBTableEngine('trigonometry', 'ts'),)
            angle_dec = Column(types.Double)
            angle_rad = Column(types.Double)
            sine = Column(types.Double)
            cosine = Column(types.Double)
            tangent = Column(types.Double)
            ts = Column(types.Timestamp, primary_key=True)

        Base.metadata.create_all(engine)
        with sqla.orm.Session(engine) as session:
            now = datetime.datetime.utcnow
            for angle_dec in range(0, 361):
                angle_rad = math.radians(angle_dec)
                session.add(Trigonometry(
                    angle_dec=angle_dec,
                    angle_rad=angle_rad,
                    sine=math.sin(angle_rad),
                    cosine=math.cos(angle_rad),
                    tangent=math.tan(angle_rad),
                    ts=now()
                ))
                time.sleep(0.0002)
            session.commit()

            columns = [col.name for col in Trigonometry.__table__.columns]
            while True:
                rs = session.execute(sqla.text(Trigonometry.__tablename__))
                if rs.rowcount:
                    print(f'rows: {rs.rowcount}')
                    for row in rs:
                        print(dict(zip(columns, map(str, row))))
                    break
    finally:
        if engine:
            engine.dispose()


if __name__ == '__main__':
    main()
