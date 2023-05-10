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
import enum
import os
import queue
import random
import threading
import time

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

import questdb_connect.dialect as qdbc
from sqlalchemy import Column, MetaData, create_engine, insert
from sqlalchemy.orm import declarative_base


class BaseEnum(enum.Enum):
    @classmethod
    def rand(cls):
        return cls._value2member_map_[random.randint(0, len(cls._member_map_) - 1)]


class Nodes(BaseEnum):
    NODE0 = 0
    NODE1 = 1


class Metrics(BaseEnum):
    CPU = 0
    RAM = 1
    HDD0 = 2
    HDD1 = 3
    NETWORK = 4


Base = declarative_base(metadata=MetaData())


class NodeMetrics(Base):
    __tablename__ = 'node_metrics'
    __table_args__ = (qdbc.QDBTableEngine(
        'node_metrics',
        'ts',
        qdbc.PartitionBy.HOUR,
        is_wal=True),)
    source = Column(qdbc.Symbol)  # Nodes
    attr_name = Column(qdbc.Symbol)  # Metrics
    attr_value = Column(qdbc.Double)
    ts = Column(qdbc.Timestamp, primary_key=True)


def produce_random_metric():
    return insert(NodeMetrics).values(
        source=Nodes.rand().name,
        attr_name=Metrics.rand().name,
        attr_value=random.random() * 100.0,
        ts=datetime.datetime.utcnow())


class QuestDBWriter(threading.Thread):
    def __init__(self, runtime_sec: float, queue_depth: int = 1000):
        super().__init__()
        self.runtime_sec = runtime_sec
        self.q = queue.Queue(5000)
        self.start()

    def write(self, insert_statement):
        self.q.put_nowait(insert_statement)

    def run(self):
        engine = create_engine('questdb://localhost:8812/main')
        try:
            Base.metadata.create_all(engine)
            conn = None
            end_time = time.time() + self.runtime_sec
            while time.time() < end_time:
                if not conn:
                    conn = engine.connect()
                try:
                    insert_stmt = self.q.get(block=False)
                    if insert_stmt is None:
                        return
                    conn.execute(insert_stmt)
                    self.q.task_done()
                except queue.Empty:
                    pass
        finally:
            if engine:
                engine.dispose()


class Sensor(threading.Thread):
    def __init__(self, writer):
        super().__init__()
        self.writer = writer
        self.start()

    def run(self):
        while self.writer.is_alive():
            self.writer.write(produce_random_metric())
            time.sleep(0.1)


if __name__ == '__main__':
    questdb_writer = QuestDBWriter(runtime_sec=5.0)
    sensors = [Sensor(questdb_writer) for _ in range(12)]
    for sen in sensors:
        sen.join()
