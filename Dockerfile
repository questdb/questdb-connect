FROM python:3.13-slim-bookworm
ENV ARCHITECTURE=x64
ENV PYTHONDONTWRITEBYTECODE 1 # Keeps Python from generating .pyc files in the container
ENV PYTHONUNBUFFERED 1 # Turns off buffering for easier container logging
ENV QUESTDB_CONNECT_HOST "host.docker.internal"

RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y --no-install-recommends install ca-certificates vim procps unzip less tar gzip iputils-ping gcc build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /app
WORKDIR /app
RUN pip install -U pip && pip install .
CMD ["python", "src/examples/sqlalchemy_orm.py"]
