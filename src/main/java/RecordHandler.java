import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
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

    private final LinkedBlockingQueue<Upload> messages;
    private final Map<TopicPartition, OffsetAndMetadata> messages_processed;

    public RecordHandler(
        LinkedBlockingQueue<Upload> messages,
        Map<TopicPartition, OffsetAndMetadata> messages_processed
    ) {
        this.messages = messages;
        this.messages_processed = messages_processed;

    }

    private static KafkaConsumer<byte[], Integer> create_pipelines_done_consumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<byte[], JsonNode> create_pipelines_sets_upload_notifications_producer() {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new KafkaProducer<>(props);
    }

    private static KafkaProducer<byte[], Integer> create_pipelines_set_upload_notifications_producer() {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        return new KafkaProducer<>(props);
    }

    private static Properties redirect_streams_props() {
        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Orchestrator.APP_NAME + "_redirect_stream");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.get(Variables.BOOTSTRAP_SERVERS));

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

        final var redirect_streams_props = redirect_streams_props();
        final var pipelines_sets_upload_notifications_producer = create_pipelines_sets_upload_notifications_producer();
        final var pipelines_set_upload_notifications_producer = create_pipelines_set_upload_notifications_producer();
        final var pipelines_done_consumer = create_pipelines_done_consumer();

        final var object_mapper = new ObjectMapper();

        ConsumerRecord<byte[], JsonNode> record;
        int pipelines_set;
        while (true) {

            while (true) {
                try {
                    var upload = messages.take();

                    record = upload.getRecord();
                    pipelines_set = upload.getPipelines_set();

                    break;
                } catch (InterruptedException ignored) {
                }
            }

            final var db_hash = record.value().get("HASH").asText();

            final var db_topic = String.format(
                "db_%s",
                db_hash
            );

            LOGGER.info("msg received");
            final var builder = new StreamsBuilder();
            builder.stream(db_topic).to(
                String.format(Constants.DATA_TO_PARSE_TOPIC_FORMAT, pipelines_set)
            );
            final var stream = new KafkaStreams(builder.build(), redirect_streams_props);
            open_streams.push(stream);
            stream.start();

            LOGGER.info("started");

            pipelines_set_upload_notifications_producer.send(
                new ProducerRecord<>(
                    String.format(Constants.PIPELINES_SET_UPLOAD_NOTIFICATIONS_TOPIC_FORMAT, pipelines_set),
                    record.value().get("ROWS").asInt()
                )
            );

            var node = object_mapper.createObjectNode();
            node.put("db_hash", db_hash);
            node.put("pipelines_set", pipelines_set);
            pipelines_sets_upload_notifications_producer.send(
                new ProducerRecord<>(
                    Constants.PIPELINES_SETS_UPLOAD_NOTIFICATIONS_TOPIC,
                    node
                )
            );

            pipelines_done_consumer.subscribe(
                Collections.singletonList(Constants.PIPELINES_SETS_DONE_TOPIC)
            );

            final int finalPipelines_set = pipelines_set;
            while (
                StreamSupport
                    .stream(
                        pipelines_done_consumer.poll(Duration.ofMillis(Long.MAX_VALUE)).spliterator(),
                        false
                    )
                    .noneMatch(r -> r.value() == finalPipelines_set)
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

            pipelines_done_consumer.unsubscribe();
        }
    }

}
