FROM python:3.9-slim

EXPOSE 8000

COPY requirements.txt requirements.txt

RUN apt update && apt install -y wait-for-it \
 && pip install -U pip && pip install -r requirements.txt

COPY adminrest /app/adminrest

WORKDIR /app/adminrest
ENV DJANGO_SETTINGS_MODULE=adminrest.settings

COPY docker-init.sh docker-entrypoint.sh /bin/

CMD docker-entrypoint.sh
