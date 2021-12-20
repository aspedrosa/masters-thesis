import org.apache.kafka.common.serialization.Deserializer;

public class IgnoreDeserializer implements Deserializer<Void> {

    public IgnoreDeserializer() {}

    @Override
    public Void deserialize(String s, byte[] bytes) {
        return null;
    }
}
