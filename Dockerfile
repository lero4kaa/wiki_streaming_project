#FROM python:3.8-slim
FROM datamechanics/spark:3.1-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3


RUN pip install pyspark
RUN pip install apscheduler

#RUN apt-get update

#RUN pip install --upgrade pip
RUN pip install requests
RUN pip install flask
RUN pip install flask_restful
RUN pip install cassandra-driver

ADD . /app
WORKDIR /app