import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

public class RecordHandler extends Thread {

    private static final Logger LOGGER = Logger.getLogger(RecordHandler.class.getName());

    private final LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>> messages;
    private final Map<TopicPartition, OffsetAndMetadata> messages_processed;

    private final KafkaConsumer<byte[], Integer> pipelines_done_consumer;

    private final Properties redirect_streams_props;

    public RecordHandler(
        LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>> messages,
        Map<TopicPartition, OffsetAndMetadata> messages_processed
    ) {
        this.messages = messages;
        this.messages_processed = messages_processed;

        this.pipelines_done_consumer = create_pipelines_done_consumer();
        this.redirect_streams_props = redirect_streams_props();
    }

    private static KafkaConsumer<byte[], Integer> create_pipelines_done_consumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantsAndVariables.get(ConstantsAndVariables.BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final var consumer = new KafkaConsumer<byte[], Integer>(props);

        consumer.subscribe(Collections.singletonList(ConstantsAndVariables.get(ConstantsAndVariables.DATA_READY_TO_SEND_TOPIC)));

        return consumer;
    }

    private static Properties redirect_streams_props() {
        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, ConstantsAndVariables.APP_NAME + "_redirect_stream");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantsAndVariables.get(ConstantsAndVariables.BOOTSTRAP_SERVERS));

        return props;
    }

    public void run() {
        final var open_streams = new ArrayDeque<KafkaStreams>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> open_streams.forEach(stream -> {
            try {
                stream.close();
            } catch (StreamsException ignored) {
            }
        })));

        while (true) {
            ConsumerRecord<byte[], JsonNode> record;

            while (true) {
                try {
                    record = messages.take();
                    break;
                } catch (InterruptedException ignored) {
                }
            }

            final var db_topic = String.format(
                "db_%s",
                record.value().get("HASH").asText()
            );

            LOGGER.info("msg received");
            final var builder = new StreamsBuilder();
            builder.stream(db_topic).to(ConstantsAndVariables.get(ConstantsAndVariables.COMMON_DATA_TOPIC));
            final var stream = new KafkaStreams(builder.build(), redirect_streams_props);
            open_streams.push(stream);
            stream.start();

            LOGGER.info("started");

            while (
                StreamSupport
                    .stream(
                        this.pipelines_done_consumer.poll(Duration.ofMillis(Long.MAX_VALUE)).spliterator(),
                        false
                    )
                    .anyMatch(r -> r.value() == 0)
            ) {
            }

            this.messages_processed.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), "")
            );

            LOGGER.info("will close");
            stream.close();
            open_streams.pop();
            stream.cleanUp();
            LOGGER.info("closed");
        }
    }

}
