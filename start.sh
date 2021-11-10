#!/bin/sh

set -x

if ! [ -f .working_instalation ] ; then
    docker-compose up -d environment-initializer

    while true ; do
        docker-compose ps environment-initializer | grep Exit
        if [ $? -ne 0 ] ; then
            sleep 2
        else
            docker-compose ps environment-initializer | grep "Exit 0"
            if [ $? -ne 0 ] ; then
                exit 1
            fi
            break
        fi
    done

    docker-compose rm -f environment-initializer

    docker-compose run --rm admin-portal-backend docker-init.sh
    docker-compose run --rm admin-portal-backend sh -c "python manage.py loaddata test_data"

    touch .working_instalation
fi

docker-compose up -d admin-portal-backend orchestrator statistics-recorder agent
docker-compose up -d ui

while true ; do
    docker-compose exec -T admin-portal-backend bash -c "curl localhost:8000"
    if [ $? -ne 0 ] ; then
        sleep 2
    else
        break
    fi
done

docker-compose up -d filter-worker sender
