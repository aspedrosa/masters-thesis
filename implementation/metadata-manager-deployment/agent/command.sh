#!/bin/env bash

set -x

MANDATORY_ENVS=(
    "AGENT_DATABASE_IDENTIFIER"
    "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL"
    "CONNECT_BOOTSTRAP_SERVERS"
    "CONNECT_ZOOKEEPER_CONNECT"
)

for MANDATORY_ENV in $MANDATORY_ENVS ; do
    if [ -z "${!MANDATORY_ENV}" ] ; then
        >&2 echo "You must define the $MANDATORY_ENV environment variable"
        exit 1
    fi
done

export CONNECT_REST_ADVERTISED_HOST_NAME=agent_$AGENT_DATABASE_IDENTIFIER
export CONNECT_CONFIG_STORAGE_TOPIC=config_storage_$AGENT_DATABASE_IDENTIFIER
export CONNECT_OFFSET_STORAGE_TOPIC=offset_storage_$AGENT_DATABASE_IDENTIFIER
export CONNECT_STATUS_STORAGE_TOPIC=status_storage_$AGENT_DATABASE_IDENTIFIER
export CONNECT_GROUP_ID=$AGENT_DATABASE_IDENTIFIER

/etc/confluent/docker/run &

echo "Waiting for Kafka Connect to start listening on kafka-connect"
while [ $(curl -s -o /dev/null -w %{http_code} http://0.0.0.0:8083/connectors) -eq 000 ] ; do

    echo -e $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://0.0.0.0:8083/connectors) " (waiting for 200)"

    sleep 5
done

nc -vz 0.0.0.0 8083

echo -e "\n--\n+> Creating Kafka AchillesResult source Connect"

CLEANUP_POLICY_CLASS=LocalMoveCleanupPolicy

if ! [ -z "$AGENT_DELETE_CLEANUP_POLICY" ] ; then
    value=$(echo $AGENT_DELETE_CLEANUP_POLICY | tr '[:upper:]' '[:lower:]')
    VALID_YES_OPTIONS=("yes" "y" "true" "1")
    for VALID_YES_OPTION in "${VALID_YES_OPTIONS[@]}" ; do
        if [ $VALID_YES_OPTION = "$value" ] ; then
            CLEANUP_POLICY_CLASS=DeleteCleanupPolicy
            break
        fi
    done
fi

sed -i "s/CLEANUP_POLICY_CLASS/$CLEANUP_POLICY_CLASS/g" /app/file_pulse/config.json
sed -i "s/AGENT_DATABASE_IDENTIFIER/$AGENT_DATABASE_IDENTIFIER/g" /app/file_pulse/config.json
sed -i "s/BOOTSTRAP_SERVERS/$CONNECT_BOOTSTRAP_SERVERS/g" /app/file_pulse/config.json

curl -sX PUT http://localhost:8083/connectors/achillesresults/config -d @/app/file_pulse/config.json --header "Content-Type: application/json"


health_check_handler &


tail -f /dev/null
