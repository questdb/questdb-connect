<a href="https://questdb.io/docs/" target="blank">
    <img alt="QuestDB Logo" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/>
</a>
<p></p>
<a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="QuestDB community Slack channel"/>
</a>

## QuestDB Connect

This repository contains the official implementation of QuestDB's dialect for [SQLAlchemy](https://www.sqlalchemy.org/),
as well as an engine specification for [Apache Superset](https://github.com/apache/superset/), using
[psycopg2](https://www.psycopg.org/) for database connectivity.

The Python module is available here:

<a href="https://pypi.org/project/questdb-connect/">
    <img src="https://pypi.org/static/images/logo-small.2a411bc6.svg" alt="PyPi"/>
    https://pypi.org/project/questdb-connect/
</a>
<p></p>

_Psycopg2_ is a widely used and trusted Python module for connecting to, and working with, QuestDB and other
PostgreSQL databases.

_SQLAlchemy_ is a SQL toolkit and ORM library for Python. It provides a high-level API for communicating with 
relational databases, including schema creation and modification. The ORM layer abstracts away the complexities 
of the database, allowing developers to work with Python objects instead of raw SQL statements.

_Apache Superset_ is an open-source business intelligence web application that enables users to visualize and 
explore data through customizable dashboards and reports. It provides a rich set of data visualizations, including 
charts, tables, and maps.

## Requirements

* **Python from 3.9 to 3.11** (superset itself use version _3.9.x_)
* **Psycopg2** `('psycopg2-binary~=2.9.6')`
* **SQLAlchemy** `('SQLAlchemy<=1.4.47')`

You need to install these packages because questdb-connect depends on them.

## Versions 0.0.X

These are versions released for testing purposes.

## Installation

You can install this package using pip:

```shell
pip install questdb-connect
```

## SQLALchemy Sample Usage

Use the QuestDB dialect by specifying it in your SQLAlchemy connection string:

```shell
questdb://admin:quest@localhost:8812/main
questdb://admin:quest@host.docker.internal:8812/main
```

From that point on use standard SQLAlchemy:

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
    __table_args__ = (qdbc.QDBTableEngine('signal', 'ts', qdbc.PartitionBy.HOUR, is_wal=True),)
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

## Superset Installation

<a href="https://superset.apache.org/docs/installation/installing-superset-from-scratch/" target="blank">
    <img alt="Apache Superset" src="https://github.com/questdb/questdb-connect/blob/main/docs/superset.png"/>
</a>

Follow the instructions available [here](https://superset.apache.org/docs/installation/installing-superset-from-scratch/).

## Contributing

This package is open-source, contributions are welcome. If you find a bug or would like to request a feature,
please open an issue on the GitHub repository. Have a look at the instructions for [developers](DEVELOPERS.md)
if you would like to push a PR.
