import datetime
import enum
import os
import random
import time

os.environ.setdefault("SQLALCHEMY_SILENCE_UBER_WARNING", "1")

import questdb_connect as qdbc
from sqlalchemy import Column, MetaData, create_engine
from sqlalchemy.orm import Session, declarative_base


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
    __tablename__ = "node_metrics"
    __table_args__ = (
        qdbc.QDBTableEngine("node_metrics", "ts", qdbc.PartitionBy.HOUR, is_wal=True),
    )
    source = Column(qdbc.Symbol)  # Nodes
    attr_name = Column(qdbc.Symbol)  # Metrics
    attr_value = Column(qdbc.Double)
    ts = Column(qdbc.Timestamp, primary_key=True)


def main(duration_sec: float = 10.0):
    end_time = time.time() + max(duration_sec - 0.5, 2.0)
    engine = create_engine("questdb://localhost:8812/main")
    session = Session(engine)
    max_batch_size = 3000
    try:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        batch_size = 0
        while time.time() < end_time:
            node = Nodes.rand()
            session.add(
                NodeMetrics(
                    source=node.name,
                    attr_name=Metrics.rand().name,
                    attr_value=random.random() * node.value * random.randint(1, 100),
                    ts=datetime.datetime.utcnow(),
                )
            )
            batch_size += 1
            if batch_size > max_batch_size:
                session.commit()
                batch_size = 0
        if batch_size > 0:
            session.commit()
    finally:
        if session:
            session.close()
        if engine:
            engine.dispose()


if __name__ == "__main__":
    main()
