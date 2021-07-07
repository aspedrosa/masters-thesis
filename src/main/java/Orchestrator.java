import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

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
        // queue used to send messages between the main thread and the record handler
        final var messages_to_process = new LinkedBlockingQueue<Upload>();
        // map with the data of the messages that were processed and can be committed by the main thread
        final var messages_to_commit = Collections.synchronizedMap(
            new HashMap<TopicPartition, OffsetAndMetadata>()
        );

        new RecordHandler(messages_to_process, messages_to_commit).start();

        final var uploads_consumer = create_upload_notifications_consumer();

        while (true) {
            uploads_consumer.poll(Duration.ofSeconds(5)).forEach(record -> {
                while (true) {
                    try {
                        messages_to_process.put(new Upload(record, 0));  // TODO have a balancing policy to distribute among existing pipelines
                        break;
                    } catch (InterruptedException ignored) {
                    }
                }
            });

            if (!messages_to_commit.isEmpty()) {
                LOGGER.info("Committing a database upload notification message");

                synchronized (messages_to_commit) {
                    uploads_consumer.commitSync(messages_to_commit);
                    messages_to_commit.clear();
                }
            }
        }
    }

    private static KafkaConsumer<byte[], JsonNode> create_upload_notifications_consumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APP_NAME + "_upload_notifications");

        final var consumer = new KafkaConsumer<byte[], JsonNode>(props);

        consumer.subscribe(Collections.singletonList(Constants.DATABASES_UPLOAD_NOTIFICATIONS_TOPIC));

        return consumer;
    }

}
