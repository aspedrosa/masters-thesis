#!/bin/sh

wait-for-it "$POSTGRES_HOST:$POSTGRES_PORT"
gunicorn adminrest.wsgi:application --bind 0.0.0.0:8000 --workers 5