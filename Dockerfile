FROM python:3.8

COPY ./transform_ingress2event_time ./transform_ingress2event_time
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt