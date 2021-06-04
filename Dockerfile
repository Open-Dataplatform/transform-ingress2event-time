FROM python:3.8

COPY ./transform_delfin_oilcable ./transform_delfin_oilcable
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt