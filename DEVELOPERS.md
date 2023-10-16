## Developer installation

If you want to contribute to this repository, you need to setup a local virtual `venv` environment:

```shell
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -e .
pip install -e '.[test]'
```

_questdb-connect_ does not have dependencies to other modules, it relies on the user to have installed
**psycopg2** and **SQLAlchemy**. When developing however, installing the `.[test]` dependencies takes
care of this.

[QuestDB 7.1.2](https://github.com/questdb/questdb/releases/tag/7.1.2), or higher, is required because
it has support for `implicit cast String -> Long256`, and must be up and running.

You can develop in your preferred IDE, and run `make test` in a terminal to check linting
(with [ruff](https://github.com/charliermarsh/ruff/)) and run the tests. Before pushing a
commit to the main branch, make sure you build the docker container and that the container
tests pass:

```shell
make test
make
make docker-test
```

Note: `make` by itself builds the docker image, then you can call `make docker-test` to run
the tests in docker. `make test` runs the tests locally and it is quicker, however CI only
runs the docker version.

## Install/Run Apache Superset from repo

These are instructions to have a running superset suitable for development.

You need to clone [superset's](https://github.com/apache/superset) repository. You can follow the
[instructions](https://superset.apache.org/docs/installation/installing-superset-from-scratch/),
which roughly equate to (depending on your environment):

- Edit file `docker/pythonpath_dev/superset_config.py` to define yout _SECRET_KEY_:
  ```python
  SUPERSET_SECRET_KEY="yourParticularSecretKeyAndMakeSureItIsSecureUnlikeThisOne"
  SECRET_KEY=SUPERSET_SECRET_KEY
  ```
- Create file `docker/requirements-local.txt` and add:
  ```shell
  questdb-connect=<version>
  ```

- And then run Apache Superset in developer mode (this takes a while):
  ```shell
  docker-compose up
  ```
- Open a browser [http://localhost:8088](http://localhost:8088)

To update `questdb-connect`:

1. `docker-compose down -v`
2. Update the version in file `./docker/requirements-local.txt`
3. `docker-compose up`

While running, the server will reload on modification of the Python and JavaScript source code.

This will be the URI for QuestDB:

```shell
questdb://admin:quest@host.docker.internal:8812/main
```

## Build questdb-connect wheel and publish it

Follow the guidelines
in [https://packaging.python.org/en/latest/tutorials/packaging-projects/](https://packaging.python.org/en/latest/tutorials/packaging-projects/).

```shell
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine

python3 -m build
python3 -m twine upload dist/*
```
