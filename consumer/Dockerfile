FROM python:3.5

ADD . /consumer
WORKDIR /consumer

RUN pip install -r requirements.txt

CMD ["python", "consume.py"]
