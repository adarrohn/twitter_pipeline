FROM python:3.5

COPY . /opt/cryptoagent/
WORKDIR /opt/cryptoagent

RUN pip install -r requirements.txt

