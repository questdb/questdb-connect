## QuestDB Connect

A [SQLAlchemy 2.x](https://docs.sqlalchemy.org/en/20/index.html) dialect for **QuestDB**, created to provide support
for [apache superset](https://github.com/apache/superset).

## Requirements

* Python from 3.8.x no higher than 3.10.x
* SQLAlchemy 2.x
* Apache Superset 2.x

## Developer installation

Create a `venv` environment:

```shell
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -e .
pip install -e '.[test]'
```

[QuestDB 7.1.2](https://github.com/questdb/questdb/releases), or higher, is required and must be up and running.
You can start QuestDB using the docker commands bellow.

## Docker commands

Build the docker image and run it:

```shell
docker build -t questdb/questdb-connect:latest .
docker run -it questdb/questdb-connect:latest bash
```

Start QuestDB and run questdb-connect tests on it:

```shell
docker-compose up
```

## Install Apache Superset

Within a directory that is a clone of superset:

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
export SUPERSET_SECRET_KEY="laRamonaEsLaMasGordaDeLasMozasDeMiPuebloRamonaTeQuiero" 
superset fab create-admin \
                    --username miguel \
                    --firstname Miguel \
                    --lastname Arregui \
                    --email miguel@questdb.io \
                    --password miguel
superset db upgrade
superset init
superset load-examples
cd superset-frontend 
npm ci
```

Directory **superset_toolkit** contains replacement files for the cloned repository:

- `Dockerfile`: At the root of the clone. Mac M1 arm64 architecture changes (this is my laptop).
- `docker-compose.yaml`: At the root of the clone. Refreshes version of nodejs.
- `pythonpath_dev`: In directory docker from the root of the clone. _SECRET_KEY_ is defined here.

To build the image:

```shell
docker build -t apache/superset:latest_dev .
```

To run Apache Superset in developer mode:

```shell
docker-compose up
```

This takes a while.

The server's root directory is:

- `/app/pythonpath`: mounted from `docker/pythonpath_dev` from the root of the clone. This directory contains
  a base configuration `superset_config.py` which we replace.
- to add python packages, for instance to test `questdb-connect` locally, add `./docker/requirements-local.txt`
  and rebuild the docker stack.
    1. Create `./docker/requirements-local.txt`
    2. Add packages
    3. Rebuild docker-compose
        1. `docker-compose down -v`
        2. `docker-compose up`
    4. Open a browser [http://localhost:8088](http://localhost:8088)

While running, the server will reload on modification of the python and JavaScript source code.
