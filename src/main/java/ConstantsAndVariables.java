import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ConstantsAndVariables {

    public static final String APP_NAME = "orchestrator";
    public static final String UPLOAD_NOTIFICATIONS_TOPIC = "UPLOAD_NOTIFICATIONS_TOPIC";
    public static final String COMMON_DATA_TOPIC = "COMMON_DATA_TOPIC";
    public static final String DATA_READY_TO_SEND_TOPIC = "DATA_READY_TO_SEND_TOPIC";
    public static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";
    private static final Logger LOGGER = Logger.getLogger(ConstantsAndVariables.class.getName());
    private static final String UPLOAD_NOTIFICATIONS_TOPIC_DEFAULT = "UPLOAD_NOTIFICATIONS";
    private static final String COMMON_DATA_TOPIC_DEFAULT = "DATA";
    private static final String DATA_READY_TO_SEND_TOPIC_DEFAULT = "DATA_READY_TO_SEND";
    private static final Map<String, String> defaults;

    static {
        defaults = new HashMap<>();
        defaults.put(UPLOAD_NOTIFICATIONS_TOPIC, UPLOAD_NOTIFICATIONS_TOPIC_DEFAULT);
        defaults.put(COMMON_DATA_TOPIC, COMMON_DATA_TOPIC_DEFAULT);
        defaults.put(DATA_READY_TO_SEND_TOPIC, DATA_READY_TO_SEND_TOPIC_DEFAULT);
    }

    public static String get(String env_var) {
        var value = System.getenv().getOrDefault(env_var, defaults.get(env_var));

        if (value == null) {
            LOGGER.severe(String.format("Environment variable %s with no default not defined", env_var));
            System.exit(2);
        }

        return value;
    }

}
