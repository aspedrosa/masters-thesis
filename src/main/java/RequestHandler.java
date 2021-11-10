import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class RequestHandler extends Thread {

    private static final Logger LOGGER = Logger.getLogger(RequestHandler.class.getName());

    private final int id;
    private final LinkedBlockingQueue<DataRequest> requests;
    private final Map<TopicPartition, OffsetAndMetadata> requests_processed;
    private final Map<Integer, KafkaStreams> open_streams;

    public RequestHandler(
        int id,
        LinkedBlockingQueue<DataRequest> requests,
        Map<TopicPartition, OffsetAndMetadata> requests_processed,
        Map<Integer, KafkaStreams> open_streams
    ) {
        this.id = id;
        this.requests = requests;
        this.requests_processed = requests_processed;
        this.open_streams= open_streams;

    }

    private static KafkaConsumer<byte[], byte[]> create_data_counter_consumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IgnoreDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Orchestrator.APP_NAME + "_data_counter");

        return new KafkaConsumer<>(props);
    }

    private static Properties redirect_streams_props() {
        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Orchestrator.APP_NAME + "_redirect_stream");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put("schema.registry.url", String.format(
            "http://%s:%s",
            Variables.get(Variables.SCHEMA_REGISTRY_HOST),
            Variables.get(Variables.SCHEMA_REGISTRY_PORT)
        ));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));

        return props;
    }

    public void run() {
        final var redirect_streams_props = redirect_streams_props();
        final var data_counter_consumer = create_data_counter_consumer();

        DataRequest request;
        while (true) {

            while (true) {
                try {
                    request = requests.take();

                    break;
                } catch (InterruptedException ignored) {
                }
            }

            final var db_hash = request.get_database_identifier();
            final var filter_worker_id = request.get_filter_worker_id();

            final var db_topic = String.format(
                "db_%s",
                db_hash
            );

            LOGGER.info(String.format("Database with hash \"%s\" sent data", db_hash));
            final var builder = new StreamsBuilder();
            final var target_data_topic = String.format(Constants.DATA_TO_PARSE_TOPIC_FORMAT, filter_worker_id);
            builder.stream(db_topic).to(target_data_topic);
            final var stream = new KafkaStreams(builder.build(), redirect_streams_props);
            open_streams.put(id, stream);
            stream.start();

            LOGGER.info(String.format("Launched the redirect stream to filter worker %d", filter_worker_id));

            data_counter_consumer.subscribe(Collections.singletonList(target_data_topic));
            AtomicInteger current_count = new AtomicInteger();
            while (current_count.get() < request.get_rows_count()) {
                data_counter_consumer
                    .poll(Duration.ofMillis(Long.MAX_VALUE))
                    .forEach(record -> current_count.getAndIncrement());
            }

            stream.close();
            stream.cleanUp();

            LOGGER.info(String.format("All data sent to filter worker %d", filter_worker_id));
            LOGGER.info(String.format("Closed the redirect stream to filter worker %d", filter_worker_id));

            this.requests_processed.put(
                request.get_topic_partition(),
                request.get_offset_and_metadata()
            );

            data_counter_consumer.commitSync();
            data_counter_consumer.unsubscribe();

            open_streams.remove(id);
        }
    }

}
