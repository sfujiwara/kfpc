FROM python:3.13-slim

RUN pip install pip==25.1.1
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY tasks tasks

ENTRYPOINT ["inv"]
