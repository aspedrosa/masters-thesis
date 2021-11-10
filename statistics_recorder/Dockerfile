FROM confluentinc/cp-kafka-connect:6.2.1

USER root

WORKDIR /app

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.2.4

ENV CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components" \
    CONNECT_GROUP_ID="statistics-recorder" \
    CONNECT_CONFIG_STORAGE_TOPIC="statistics-recorder-storage" \
    CONNECT_OFFSET_STORAGE_TOPIC="statistics-recorder-offset" \
    CONNECT_STATUS_STORAGE_TOPIC="statistics-recorder-status" \
    CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
    CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter \
    CONNECT_REST_ADVERTISED_HOST_NAME=localhost

COPY *.json command.sh /app/

CMD "/app/command.sh"
