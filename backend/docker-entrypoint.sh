#!/bin/sh

wait-for-it "$POSTGRES_HOST:$POSTGRES_PORT"
wait-for-it "$KSQLDB_HOST:$KSQLDB_PORT"
wait-for-it "$SCHEMA_REGISTRY_HOST:$SCHEMA_REGISTRY_PORT"

gunicorn adminrest.wsgi:application --bind 0.0.0.0:8000 --workers 5