FROM streamthoughts/kafka-connect-file-pulse:2.4.0

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN mkdir /app \
 && mkdir /app/healthcheck_replier \
 && mkdir /app/file_pulse

COPY command.sh /app
COPY achilles.json /app/file_pulse/config.json

CMD "/app/command.sh"

# TODO copy healthcheck_replier app
