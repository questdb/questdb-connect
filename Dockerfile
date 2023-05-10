#
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2023 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
FROM python:3.10-slim-buster
ENV ARCHITECTURE=x64
ENV PYTHONDONTWRITEBYTECODE 1 # Keeps Python from generating .pyc files in the container
ENV PYTHONUNBUFFERED 1 # Turns off buffering for easier container logging
ENV SQLALCHEMY_SILENCE_UBER_WARNING 1 # because we really should upgrade to SQLAlchemy 2.x
ENV QUESTDB_CONNECT_HOST "host.docker.internal"

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y --no-install-recommends install syslog-ng ca-certificates vim procps unzip less tar gzip iputils-ping gcc build-essential
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

COPY . /app
WORKDIR /app
RUN pip install -U pip && pip install psycopg2-binary && pip install 'SQLAlchemy<=1.4.47' && pip install .
CMD ["python", "src/examples/sqlalchemy_orm.py"]
