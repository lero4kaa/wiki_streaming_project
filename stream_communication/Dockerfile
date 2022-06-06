FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip
RUN pip install requests
RUN pip install cassandra-driver
RUN pip install pyspark


WORKDIR /python-docker

COPY ./stream_communication.py .
COPY ./Communicator.py .

CMD ["python3", "stream_communication.py"]