FROM python:3.9-slim

EXPOSE 8000

COPY requirements.txt requirements.txt

RUN apt update && apt install -y wait-for-it curl \
 && pip install -U pip && pip install -r requirements.txt \
 && rm -rf ~/.cache/pip && rm -rf /var/lib/apt/lists/*

COPY adminrest /app/adminrest

WORKDIR /app/adminrest
ENV DJANGO_SETTINGS_MODULE=adminrest.settings

COPY docker-init.sh docker-entrypoint.sh /usr/local/bin/

CMD docker-entrypoint.sh
