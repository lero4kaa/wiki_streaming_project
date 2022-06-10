FROM python:3.8-slim

RUN apt-get update

RUN pip install --upgrade pip
RUN pip install requests
RUN pip install flask
RUN pip install flask_restful
RUN pip install cassandra-driver

ADD . /app
WORKDIR /app
