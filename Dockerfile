FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt /app

RUN pip install -U pip && pip install -r requirements.txt

COPY *.py /app/

CMD python main.py
