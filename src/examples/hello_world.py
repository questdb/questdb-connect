import datetime
import os

os.environ.setdefault("SQLALCHEMY_SILENCE_UBER_WARNING", "1")

import questdb_connect as qdbc
from sqlalchemy import Column, MetaData, create_engine, insert
from sqlalchemy.orm import declarative_base

Base = declarative_base(metadata=MetaData())


class Signal(Base):
    __tablename__ = "signal"
    __table_args__ = (
        qdbc.QDBTableEngine("signal", "ts", qdbc.PartitionBy.HOUR, is_wal=True),
    )
    source = Column(qdbc.Symbol)
    value = Column(qdbc.Double)
    ts = Column(qdbc.Timestamp, primary_key=True)


def main():
    engine = create_engine("questdb://localhost:8812/main")
    try:
        Base.metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(
                insert(Signal).values(
                    source="coconut", value=16.88993244, ts=datetime.datetime.utcnow()
                )
            )
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    main()
