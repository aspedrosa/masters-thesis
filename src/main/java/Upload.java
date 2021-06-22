import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Upload {
    private final ConsumerRecord<byte[], JsonNode> record;

    private final int pipelines_set;

    public Upload(ConsumerRecord<byte[], JsonNode> record, int pipelines_set) {
        this.record = record;
        this.pipelines_set = pipelines_set;
    }

    public ConsumerRecord<byte[], JsonNode> getRecord() {
        return record;
    }

    public int getPipelines_set() {
        return pipelines_set;
    }
}
