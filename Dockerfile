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
RUN pip install -U pip && pip install psycopg2-binary 'SQLAlchemy<=1.4.47' .
CMD ["python", "src/examples/sqlalchemy_orm.py"]
