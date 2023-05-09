## Developer installation

Create a `venv` environment:

```shell
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -e .
pip install -e '.[test]'
```

[QuestDB 7.1.2](https://github.com/questdb/questdb/releases), or higher, is required because it has support for 
implicit cast String -> Long256, and must be up and running. You can start QuestDB using the docker commands bellow.

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

## Install/Run Apache Superset from repo

As per the instructions [here](https://superset.apache.org/docs/installation/installing-superset-from-scratch/), within 
a directory that is a clone of superset:

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

superset run -p 8088 --with-threads --reload --debugger
```

## Install/Run Apache Superset from docker

Directory **superset_toolkit** contains replacement files for the cloned repository:

- `Dockerfile`: At the root of the clone. Mac M1 arm64 architecture changes (this is my laptop).
- `docker-compose.yaml`: At the root of the clone. Refreshes version of nodejs.
- `pythonpath_dev`: In directory docker from the root of the clone. _SECRET_KEY_ is defined here.

To build the image first install superset following the steps above, then build the docker image:

```shell
docker build -t apache/superset:latest-dev .
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
