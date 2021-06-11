import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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

public class RecordHandler extends Thread {

    private final LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>> messages;
    private final Map<TopicPartition, OffsetAndMetadata> messages_processed;

    private final KafkaConsumer<byte[], byte[]> data_ready_consumer;

    private final Properties redirect_streams_props;

    public RecordHandler(
        LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>> messages,
        Map<TopicPartition, OffsetAndMetadata> messages_processed
    ) {
        this.messages = messages;
        this.messages_processed = messages_processed;

        this.data_ready_consumer = create_data_ready_consumer();
        this.redirect_streams_props = redirect_streams_props();
    }

    private static KafkaConsumer<byte[], byte[]> create_data_ready_consumer() {
        final var env = System.getenv();
        final var DATA_READY_TO_SEND_TOPIC = env.getOrDefault("DATA_READY_TO_SEND_TOPIC", "DATA_READY_TO_SEND");

        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Orchestrator.APP_NAME + "_data_ready");

        final var consumer = new KafkaConsumer<byte[], byte[]>(props);

        consumer.subscribe(Collections.singletonList(DATA_READY_TO_SEND_TOPIC));

        return consumer;
    }

    private static Properties redirect_streams_props() {
        final var env = System.getenv();

        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Orchestrator.APP_NAME + "_redirect_stream");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS"));

        return props;
    }

    public void run() {
        final var logger = Logger.getLogger(RecordHandler.class.getName());

        final var COMMON_DATA_TOPIC = System.getenv().getOrDefault("COMMON_DATA_TOPIC", "DATA");

        final var open_streams = new ArrayDeque<KafkaStreams>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("closing streams");
            open_streams.forEach(stream -> {
                try {
                    stream.close();
                } catch (StreamsException ignored) {
                }
            });
        }));

        while (true) {
            ConsumerRecord<byte[], JsonNode> record;

            while (true) {
                try {
                    record = messages.take();
                    break;
                } catch (InterruptedException ignored) {
                }
            }

            final var db_topic = "achillesresults";  // TODO get this from the message

            logger.info("msg received");
            final var builder = new StreamsBuilder();
            builder.stream(db_topic).to(COMMON_DATA_TOPIC);
            final var stream = new KafkaStreams(builder.build(), redirect_streams_props);
            open_streams.push(stream);
            stream.start();

            logger.info("started");

            final int pipelines_online = 2;  // TODO get this from a common db
            int pipelines_done = 0;
            while (pipelines_done < pipelines_online) {
                pipelines_done += data_ready_consumer.poll(Duration.ofMillis(Long.MAX_VALUE)).count();
            }

            this.messages_processed.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), "")
            );

            logger.info("will close");
            stream.close();
            open_streams.pop();
            stream.cleanUp();
            logger.info("closed");
        }
    }

}
