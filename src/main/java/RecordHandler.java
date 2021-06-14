import avro.shaded.com.google.common.collect.Sets;
import com.fasterxml.jackson.databind.JsonDeserializer;
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
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class RecordHandler extends Thread {

    private static final Logger LOGGER = Logger.getLogger(RecordHandler.class.getName());

    private final LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>> messages;
    private final Map<TopicPartition, OffsetAndMetadata> messages_processed;

    private final KafkaConsumer<byte[], JsonNode> data_ready_consumer;

    private final Properties redirect_streams_props;

    private final DatabaseProxy db_proxy;

    private final Set<Long> pipelines_done;

    public RecordHandler(
        LinkedBlockingQueue<ConsumerRecord<byte[], JsonNode>> messages,
        Map<TopicPartition, OffsetAndMetadata> messages_processed,
        DatabaseProxy db_proxy
    ) {
        this.messages = messages;
        this.messages_processed = messages_processed;

        this.data_ready_consumer = create_data_ready_consumer();
        this.redirect_streams_props = redirect_streams_props();

        this.db_proxy = db_proxy;

        pipelines_done = new HashSet<>();
    }

    private static KafkaConsumer<byte[], JsonNode> create_data_ready_consumer() {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantsAndVariables.get(ConstantsAndVariables.BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConstantsAndVariables.APP_NAME + "_data_ready");

        final var consumer = new KafkaConsumer<byte[], JsonNode>(props);

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

            // get pipelines active
            Set<Long> online_at_start = db_proxy.pipelines_ids();

            final var db_topic = String.format(
                "db_%s",
                record.value().get("hash").asText()
            );

            LOGGER.info("msg received");
            final var builder = new StreamsBuilder();
            builder.stream(db_topic).to(ConstantsAndVariables.get(ConstantsAndVariables.COMMON_DATA_TOPIC));
            final var stream = new KafkaStreams(builder.build(), redirect_streams_props);
            open_streams.push(stream);
            stream.start();

            LOGGER.info("started");

            while (all_done(online_at_start)) {
                for (var ready_record : data_ready_consumer.poll(Duration.ofMillis(Long.MAX_VALUE))) {
                    this.pipelines_done.add(ready_record.value().get("sub_id").asLong());
                }
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

            this.pipelines_done.clear();
        }
    }

    private boolean all_done(final Set<Long> online_at_start) {
        var currently_active = db_proxy.pipelines_ids();

        online_at_start.retainAll(currently_active);

        return Sets.intersection(online_at_start, this.pipelines_done).size() == online_at_start.size();
    }

}
