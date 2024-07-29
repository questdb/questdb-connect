import pytest
from _pytest.outcomes import fail
from psycopg2 import OperationalError
from sqlalchemy import create_engine


def test_user(test_engine, test_model):
    engine = create_engine("questdb://admin:quest@localhost:8812/qdb")
    engine.connect()

    engine = create_engine("questdb://user1:quest@localhost:8812/qdb")
    with pytest.raises(OperationalError) as exc_info:
        engine.connect()
    if not str(exc_info.value).__contains__("ERROR:  invalid username/password"):
        fail()
