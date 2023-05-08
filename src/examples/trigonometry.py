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

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

import time

import questdb_connect.dialect as qdbc
from sqlalchemy import Column, MetaData, text
from sqlalchemy.orm import Session, declarative_base

from examples import CONNECTION_ATTRS

Base = declarative_base(metadata=MetaData())


class Trigonometry(Base):
    __tablename__ = 'trigonometry'
    __table_args__ = (qdbc.QDBTableEngine('trigonometry', 'ts'),)
    angle_dec = Column(qdbc.Double)
    angle_rad = Column(qdbc.Double)
    sine = Column(qdbc.Double)
    cosine = Column(qdbc.Double)
    tangent = Column(qdbc.Double)
    ts = Column(qdbc.Timestamp, primary_key=True)


def main():
    engine = qdbc.create_engine(**CONNECTION_ATTRS)
    try:
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            now = datetime.datetime.utcnow
            for angle_dec in range(0, 361):
                angle_rad = math.radians(angle_dec)
                session.add(Trigonometry(
                    angle_dec=angle_dec,
                    angle_rad=angle_rad,
                    sine=math.sin(angle_rad),
                    cosine=math.cos(angle_rad),
                    tangent=math.tan(angle_rad),
                    ts=now()))
                time.sleep(0.0002)
            session.commit()

            columns = [col.name for col in Trigonometry.__table__.columns]
            while True:
                rs = session.execute(text(Trigonometry.__tablename__))
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
