import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class DataRequest {

    private final ConsumerRecord<byte[], JsonNode> record;

    public DataRequest(ConsumerRecord<byte[], JsonNode> record) {
        this.record = record;
    }

    public int get_filter_worker_id() {
        return record.value().get("filter_worker_id").asInt();
    }

    public String get_database_identifier() {
        return record.value().get("database_identifier").asText();
    }

    public int get_rows_count() {
        return record.value().get("rows").asInt();
    }

    public TopicPartition get_topic_partition() {
        return new TopicPartition(record.topic(), record.partition());
    }

    public OffsetAndMetadata get_offset_and_metadata() {
        return new OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), null);
    }

}
