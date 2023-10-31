from examples import hello_world, psycopg2_connect, server_utilisation, sqlalchemy_orm, sqlalchemy_raw


def test_hello_world():
    hello_world.main()


def test_psycopg2_connect():
    psycopg2_connect.main()


def test_server_utilisation():
    server_utilisation.main(duration_sec=2.0)


def test_sqlalchemy_orm():
    sqlalchemy_orm.main()


def test_sqlalchemy_raw():
    sqlalchemy_raw.main()
