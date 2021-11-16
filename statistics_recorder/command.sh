#!/bin/bash

set -ex

/etc/confluent/docker/run &

echo "Waiting for Kafka Connect to start listening on kafka-connect"
while [ $(curl -s -o /dev/null -w %{http_code} http://0.0.0.0:8083/connectors) -eq 000 ] ; do

    echo -e $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://0.0.0.0:8083/connectors) " (waiting for 200)"

    sleep 5
done

nc -vz 0.0.0.0 8083

echo -e "\n--\n+> Creating Kafka sink Connectors"

for variable in DB_HOST DB_PORT DB_DATABASE DB_USER DB_PASSWORD ; do
    sed -i "s/$variable/${!variable}/" /app/healthcheks.json
    sed -i "s/$variable/${!variable}/" /app/applicationdatasent.json
    sed -i "s/$variable/${!variable}/" /app/databaseuploadeddata.json
done

curl -sX PUT http://localhost:8083/connectors/healthcheks/config -d @/app/healthcheks.json --header "Content-Type: application/json"
curl -sX PUT http://localhost:8083/connectors/applicationdatasent/config -d @/app/applicationdatasent.json --header "Content-Type: application/json"
curl -sX PUT http://localhost:8083/connectors/databaseuploadeddata/config -d @/app/databaseuploadeddata.json --header "Content-Type: application/json"

tail -f /dev/null
