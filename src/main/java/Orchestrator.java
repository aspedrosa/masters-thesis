import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Properties;

public class Orchestrator {

    private static final String APP_NAME = "orchestrator";

    public static void main(final String[] args) {
        final var env = System.getenv();
        final var COMMON_DATA_TOPIC = env.getOrDefault("COMMON_DATA_TOPIC", "DATA");

        final var uploads_consumer = create_upload_notifications_consumer();
        final var data_ready_consumer = create_data_ready_consumer();

        final var redirect_streams_props = redirect_streams_props();

        final var open_streams = new ArrayDeque<KafkaStreams>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("asdfasdf");
            try {
                open_streams.forEach(KafkaStreams::close);
            }
            catch (StreamsException e) {
                e.printStackTrace();
            }
        }));

        while (true) {
            for (var record : uploads_consumer.poll(Duration.ofMillis(Long.MAX_VALUE))) {
                final var db_topic = "achillesresults";  // TODO get this from the message

                final var builder = new StreamsBuilder();
                builder.stream(db_topic).to(COMMON_DATA_TOPIC);
                final var stream = new KafkaStreams(builder.build(), redirect_streams_props);
                open_streams.push(stream);
                stream.start();

                final int pipelines_online = 2;  // TODO get this from a common db

                int pipelines_done = 0;
                while (pipelines_done < pipelines_online) {
                    pipelines_done += data_ready_consumer.poll(Duration.ofMillis(Long.MAX_VALUE)).count();
                }

                stream.close();
                open_streams.pop();
                stream.cleanUp();
            }
        }
    }

    private static KafkaConsumer<byte[], JsonNode> create_upload_notifications_consumer() {
        final var env = System.getenv();
        final var UPLOAD_NOTIFICATION_TOPIC = env.getOrDefault("UPLOAD_NOTIFICATION_TOPIC", "UPLOAD_NOTIFICATIONS");

        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APP_NAME + "_upload_notifications");

        final var consumer = new KafkaConsumer<byte[], JsonNode>(props);

        consumer.subscribe(List.of(UPLOAD_NOTIFICATION_TOPIC));

        return consumer;
    }

    private static KafkaConsumer<byte[], byte[]> create_data_ready_consumer() {
        final var env = System.getenv();
        final var DATA_READY_TO_SEND_TOPIC = env.getOrDefault("DATA_READY_TO_SEND_TOPIC", "DATA_READY_TO_SEND");

        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APP_NAME + "_data_ready");

        final var consumer = new KafkaConsumer<byte[], byte[]>(props);

        consumer.subscribe(List.of(DATA_READY_TO_SEND_TOPIC));

        return consumer;
    }

    private static Properties redirect_streams_props() {
        final var env = System.getenv();

        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME + "_redirect_stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS"));

        return props;
    }
}
