import os

CONNECTION_ATTRS = {
    "host": os.environ.get("QUESTDB_CONNECT_HOST", "localhost"),
    "port": int(os.environ.get("QUESTDB_CONNECT_PORT", "8812")),
    "username": os.environ.get("QUESTDB_CONNECT_USER", "admin"),
    "password": os.environ.get("QUESTDB_CONNECT_PASSWORD", "quest"),
}
