FROM python

RUN pip install pip==22.1.2
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY tasks tasks

ENTRYPOINT ["inv"]
