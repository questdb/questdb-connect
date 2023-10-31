import sqlalchemy as sqla


def main():
    print(f"SqlAlchemy {sqla.__version__}")

    table_name = "sqlalchemy_raw"
    engine = sqla.create_engine("questdb://localhost:8812/main", echo=True, future=True)
    try:
        with engine.connect() as conn:
            conn.execute(sqla.text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.execute(
                sqla.text(f"CREATE TABLE IF NOT EXISTS {table_name} (x int, y int)")
            )
            conn.execute(
                sqla.text(f"INSERT INTO {table_name} (x, y) VALUES (:x, :y)"),
                [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
            )
            conn.commit()

            result = conn.execute(
                sqla.text(f"SELECT x, y FROM {table_name} WHERE y > :y"), {"y": 2}
            )
            for row in result:
                print(f"x: {row.x}  y: {row.y}")

            result = conn.execute(sqla.text(f"SELECT x, y FROM {table_name}"))
            for dict_row in result.mappings():
                x = dict_row["x"]
                y = dict_row["y"]
                print(f"x: {x}  y: {y}")
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    main()
