#!/bin/env bash

/etc/confluent/docker/run &

echo "Waiting for Kafka Connect to start listening on kafka-connect"
while [ $(curl -s -o /dev/null -w %{http_code} http://0.0.0.0:$CONNECT_REST_PORT/connectors) -eq 000 ] ; do

    echo -e $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://0.0.0.0:$CONNECT_REST_PORT/connectors) " (waiting for 200)"

    sleep 5
done

nc -vz 0.0.0.0 $CONNECT_REST_PORT

echo -e "\n--\n+> Creating Kafka AchillesResult source Connect"

curl -sX PUT http://localhost:8083/connectors/achillesresults/config -d @/app/file_pulse/config.json --header "Content-Type: application/json"

tail -f /dev/null
