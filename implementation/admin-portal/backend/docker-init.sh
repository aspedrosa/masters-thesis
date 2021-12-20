#!/bin/sh

set -e

wait-for-it "$POSTGRES_HOST:$POSTGRES_PORT"

echo "Applying migrations"
python manage.py migrate

echo "Creating super user"
python manage.py createsuperuser
