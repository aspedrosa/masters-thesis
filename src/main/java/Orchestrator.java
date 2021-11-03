import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsException;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class Orchestrator {

    public static final String APP_NAME = "orchestrator";

    private static final Logger LOGGER = Logger.getLogger(Orchestrator.class.getName());

    public static void main(final String[] args) {
        // - create shared structs
        // queue used to send messages between the main thread and the record handler
        final var requests_to_process = new LinkedBlockingQueue<DataRequest>();
        // map with the data of the messages that were processed and can be committed by the main thread
        final var messages_to_commit = Collections.synchronizedMap(
            new HashMap<TopicPartition, OffsetAndMetadata>()
        );
        final var open_streams = Collections.synchronizedMap(
            new HashMap<Integer, KafkaStreams>()
        );
        // - close open streams when closing the application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> open_streams.values().forEach(stream -> {
            try {
                stream.close();
            } catch (StreamsException ignored) {
            }
        })));

        // - launch request handlers
        {
            Integer request_handlers_count = null;
            try {
                request_handlers_count = Integer.parseInt(
                    Variables.get_with_default(
                        "REQUEST_HANDLERS_COUNT", Runtime.getRuntime().availableProcessors()+""
                    )
                );
            } catch (NumberFormatException e) {
                LOGGER.severe("Invalid integer format on env variable REQUEST_HANDLERS_COUNT");
                System.exit(2);
            }

            if (request_handlers_count <= 0 || request_handlers_count > Runtime.getRuntime().availableProcessors()) {
                LOGGER.severe(String.format(
                    "REQUEST_HANDLERS_COUNT must be an integer greater than zero and lower than %d",
                    Runtime.getRuntime().availableProcessors() + 1
                ));
                System.exit(2);
            }

            for (int i = 0; i < request_handlers_count; i++) {
                new RequestHandler(i, requests_to_process, messages_to_commit, open_streams).start();
            }
        }

        final var data_requests_consumer = create_data_requests_consumer();

        while (true) {
            data_requests_consumer.poll(Duration.ofSeconds(5)).forEach(record -> {
                while (true) {
                    try {
                        requests_to_process.put(new DataRequest(record));
                        break;
                    } catch (InterruptedException ignored) {
                    }
                }
            });

            if (!messages_to_commit.isEmpty()) {
                LOGGER.info("Committing a database upload notification message");

                synchronized (messages_to_commit) {
                    data_requests_consumer.commitSync(messages_to_commit);
                    messages_to_commit.clear();
                }
            }
        }
    }

    private static KafkaConsumer<byte[], JsonNode> create_data_requests_consumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APP_NAME);

        final var consumer = new KafkaConsumer<byte[], JsonNode>(props);

        consumer.subscribe(Collections.singletonList(Constants.DATA_REQUEST_TOPIC));

        return consumer;
    }

}
