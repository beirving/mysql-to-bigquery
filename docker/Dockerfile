FROM python:3.7-buster
RUN apt-get update \
    && apt-get install openssl \
    && apt-get install ca-certificates
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY /src ./src
COPY /credentials ./credentials
COPY setup.py setup.py
RUN python setup.py install