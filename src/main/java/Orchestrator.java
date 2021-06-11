import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class Orchestrator {

    public static final String APP_NAME = "orchestrator";

    public static void main(final String[] args) {
        final var messages_to_process = new LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>>();
        final var messages_to_commit = Collections.synchronizedMap(
            new HashMap<TopicPartition, OffsetAndMetadata>()
        );

        new RecordHandler(messages_to_process, messages_to_commit).start();

        final var uploads_consumer = create_upload_notifications_consumer();

        final var logger = Logger.getLogger(Orchestrator.class.getName());

        while (true) {
            uploads_consumer.poll(Duration.ofSeconds(5)).forEach(record -> {
                while (true) {
                    try {
                        messages_to_process.put(record);
                        break;
                    } catch (InterruptedException ignored) {
                    }
                }
            });

            if (!messages_to_commit.isEmpty()) {
                logger.info("commiting");
                synchronized (messages_to_commit) {
                    uploads_consumer.commitSync(messages_to_commit);
                    messages_to_commit.clear();
                }
            }
        }
    }

    private static KafkaConsumer<byte[], JsonNode> create_upload_notifications_consumer() {
        final var env = System.getenv();
        final var UPLOAD_NOTIFICATION_TOPIC = env.getOrDefault("UPLOAD_NOTIFICATION_TOPIC", "UPLOAD_NOTIFICATIONS");

        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APP_NAME + "_upload_notifications");

        final var consumer = new KafkaConsumer<byte[], JsonNode>(props);

        consumer.subscribe(List.of(UPLOAD_NOTIFICATION_TOPIC));

        return consumer;
    }

}
