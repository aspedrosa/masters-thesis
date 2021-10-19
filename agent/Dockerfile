FROM streamthoughts/kafka-connect-file-pulse:2.4.0

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components" \
    CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
    CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter \
    CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1
    # maybe these replication factor are not required if the topic is already created.
    #  which could be created only after the admin register the database on the admin dashboard


RUN mkdir /app \
 && mkdir /app/healthcheck_replier \
 && mkdir /app/file_pulse

COPY command.sh /app
COPY achilles.json /app/file_pulse/config.json

CMD "/app/command.sh"

# TODO copy healthcheck_replier app
