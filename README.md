## QuestDB Connect

This Python 3 module provides a [SQLAlchemy 1.4](https://docs.sqlalchemy.org/en/14/index.html) dialect
implementation for **QuestDB**, as well as an [Apache Superset 2.0](https://github.com/apache/superset)
engine specification, using [psycopg2](https://www.psycopg.org/).

Psycopg2 is a Python module that provides a way to connect to and interact with PostgreSQL databases. It
is a widely used and popular library due to its comprehensive support for the PostgreSQL database system,
and its high level of reliability and stability.

SQLAlchemy is an open-source SQL toolkit and Object-Relational Mapping (ORM) library for Python. It provides
a set of high-level API for communicating with relational databases, including an SQL expression language,
schema creation and modification, and database connection management. SQLAlchemy provides a set of core
utilities and an ORM layer that abstracts away the details of the database, allowing you to work with Python
objects instead of raw SQL statements.

Apache Superset is an open-source business intelligence web application that enables users to explore and
visualize data. It offers a rich set of data visualizations, including charts, tables, and maps, that are
used for creating custom dashboards and reports.

## Requirements

* Python from 3.8.x to 3.10.x
* SQLAlchemy 1.4.x to 2.0.x

## Installation

You can install this package using pip:

```shell
pip install questdb-connect
```

## Sample Usage

Use the QuestDB dialect by specifying it in your SQLAlchemy connection string, 
from that point on use SQLAlchemy:

```python
import datetime
import os

os.environ.setdefault('SQLALCHEMY_SILENCE_UBER_WARNING', '1')

import questdb_connect.dialect as qdbc
from sqlalchemy import Column, MetaData, create_engine, insert
from sqlalchemy.orm import declarative_base

Base = declarative_base(metadata=MetaData())


class Signal(Base):
    __tablename__ = 'signal'
    __table_args__ = (qdbc.QDBTableEngine(
        'signal',
        'ts',
        qdbc.PartitionBy.HOUR,
        is_wal=True),)
    source = Column(qdbc.Symbol)
    value = Column(qdbc.Double)
    ts = Column(qdbc.Timestamp, primary_key=True)


def main():
    engine = create_engine('questdb://localhost:8812/main')
    try:
        Base.metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(insert(Signal).values(
                source='coconut',
                value=16.88993244,
                ts=datetime.datetime.utcnow()
            ))
    finally:
        if engine:
            engine.dispose()


if __name__ == '__main__':
    main()
```

## Contributing

This package is open-source, contributions are welcome. If you find a bug or would like to request a feature,
please open an issue on the GitHub repository.
