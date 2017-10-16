FROM python:3.5

ADD . /producer
WORKDIR /producer

RUN pip install -r requirements.txt

CMD ["python", "produce.py"]
