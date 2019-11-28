FROM python:3.7

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

WORKDIR /opt
ADD kafkian_example /opt/kafkian_example
