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
**psycopg2**, **SQLAlchemy** and **superset**. When developing however, installing the `.[test]` 
dependencies takes care of this.

[QuestDB 7.2](https://github.com/questdb/questdb/releases), or higher, is required because it has 
support for `implicit cast String -> Long256`, and must be up and running.

You can develop in your preferred IDE, and run `make test` in a terminal to check linting and
run the tests. Before pushing a commit to the main branch, make sure you build the docker container
and that the container tests pass:

```shell
make
make docker-test
```

## Install/Run Apache Superset from repo

These are instructions to have a running superset suitable for development.

You need to clone [superset's](https://github.com/apache/superset) repository. You can follow the 
[instructions](https://superset.apache.org/docs/installation/installing-superset-from-scratch/), 
which roughly equate to (depending on your environment):

**superset cannot run on python > 3.10.x**

```shell
pyenv install 3.10.10
pyenv local 3.10.10
python3 --version
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements/local.txt
pip install -e .
pip install sqlparse=='0.4.3'
export SUPERSET_SECRET_KEY="yourParticularSecretKeyAndMakeSureItIsSecureUnlikeThisOne" 
superset fab create-admin
superset db upgrade
superset init
superset load-examples
cd superset-frontend 
npm ci
npm run build
cd ..
```

For me, the above process does not always go 100% well because I am on a Mac M1, so
what I proceed to do next is to build a superset docker image. The above steps prime
this step, so make sure you follow them.

Directory **superset_toolkit** contains replacement files for the cloned superset repository:

- `Dockerfile`: At the root of the clone. Mac M1 arm64 architecture changes.
- `docker-compose.yaml`: At the root of the clone. Refreshes version of nodejs.
- `docker/pythonpath_dev/superset_config.py`: _SECRET_KEY_ is defined here:
  ```python
  SUPERSET_SECRET_KEY="yourParticularSecretKeyAndMakeSureItIsSecureUnlikeThisOne"
  SECRET_KEY=SUPERSET_SECRET_KEY
  ```
- `docker/requirements-local.txt`: This file needs to be created and must contain (where 
  NN is the latest [release](https://pypi.org/project/questdb-connect/)):
  ```shell
  questdb-connect=0.0.NN
  ```

Once the superset repo has been adapted to meet the above list's criteria, you can build
the image like this:

```shell
docker build -t apache/superset:latest-dev .
```

And then run Apache Superset in developer mode:

```shell
docker-compose up
```

This takes a while. The server's root directory is:

- `/app/pythonpath`: mounted from `docker/pythonpath_dev`.
- to add any extra local python packages, for instance `questdb-connect`, add them to 
  `./docker/requirements-local.txt` and rebuild the docker stack:
    1. Create `./docker/requirements-local.txt`
    2. Add packages
    3. Rebuild docker-compose
        1. `docker-compose down -v`
        2. `docker-compose up`
    4. Open a browser [http://localhost:8088](http://localhost:8088)

While running, the server will reload on modification of the python and JavaScript source code.

This will be the URI for QuestDB:

```shell
questdb://admin:quest@host.docker.internal:8812/main
```

## Build questdb-connect wheel and publish it

Follow the guidelines in [https://packaging.python.org/en/latest/tutorials/packaging-projects/](https://packaging.python.org/en/latest/tutorials/packaging-projects/).


```shell
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine

python3 -m build
python3 -m twine upload dist/*
```
