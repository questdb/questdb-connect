#
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2022 QuestDB
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

.DEFAULT_GOAL := docker-build

IMAGE 			  ?= questdb/questdb-connect
SHORT_COMMIT_HASH ?= $(shell git rev-parse --short HEAD)
BRANCH 			  ?= $(shell git rev-parse --abbrev-ref HEAD)

DB_HOSTNAME		  ?= 127.0.0.1

ifeq (, $(shell which docker))
$(error "No docker in $(PATH), consider checking out https://docker.io/ for info")
endif

ifeq (, $(shell which curl))
$(error "No curl in $(PATH)")
endif

docker-build:
	docker build -t questdb/questdb-connect:latest .

docker-run:
	docker run -it questdb/questdb-connect:latest

docker-push:
	docker push questdb/questdb-connect:latest

compose-up:
	docker-compose up

compose-down:
	docker-compose down --remove-orphans
	echo "y" | docker container prune
	echo "y" | docker volume prune

docker-test:
	docker run -e QUESTDB_CONNECT_HOST='host.docker.internal' -e SQLALCHEMY_SILENCE_UBER_WARNING=1 questdb/questdb-connect:latest

test:
	python3 -m pytest
	python3 -m black src
	python3 -m ruff check src/questdb_connect --fix
	python3 -m ruff check src/examples --fix
	python3 -m ruff check tests --fix

-include ../Mk/phonies
