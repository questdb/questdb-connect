"""Tests for SQLAlchemy 2.0 compatibility.

These tests verify the SA 2.0 migration changes work correctly.
"""
import sqlalchemy

import questdb_connect as qdbc
from questdb_connect.dialect import QuestDBDialect


def test_import_dbapi():
    """import_dbapi() must exist and return the questdb_connect module."""
    dbapi = QuestDBDialect.import_dbapi()
    assert dbapi is qdbc


def test_create_engine_no_deprecated_params(test_config):
    """create_engine() must not pass deprecated params on SA 2.0."""
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        engine = qdbc.create_engine(
            test_config.host,
            test_config.port,
            test_config.username,
            test_config.password,
            test_config.database,
        )
        sa_dep = [x for x in w if issubclass(x.category, DeprecationWarning)
                  and ("implicit_returning" in str(x.message) or "future" in str(x.message))]
        assert len(sa_dep) == 0, f"Unexpected deprecation warnings: {sa_dep}"
        engine.dispose()


def test_create_superset_engine_no_deprecated_params(test_config):
    """create_superset_engine() must not pass deprecated params on SA 2.0."""
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        engine = qdbc.create_superset_engine(
            test_config.host,
            test_config.port,
            test_config.username,
            test_config.password,
            test_config.database,
        )
        sa_dep = [x for x in w if issubclass(x.category, DeprecationWarning)
                  and ("implicit_returning" in str(x.message) or "future" in str(x.message))]
        assert len(sa_dep) == 0, f"Unexpected deprecation warnings: {sa_dep}"
        engine.dispose()


def test_sa2_required():
    """SQLAlchemy 2.0+ must be installed."""
    major = int(sqlalchemy.__version__.split(".")[0])
    assert major >= 2
